package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"time"

	"github.com/kardianos/service"
)

const version = "1.0.0"

var (
	srvMu   sync.Mutex
	httpSrv *http.Server
)

func startHTTP() error {
	c := getConfig()
	ln, err := net.Listen("tcp", c.Listen)
	if err != nil {
		return err
	}
	s := &http.Server{Handler: server{}, ReadHeaderTimeout: 15 * time.Second, IdleTimeout: 120 * time.Second}
	srvMu.Lock()
	httpSrv = s
	srvMu.Unlock()
	log.Printf("[http] ProxyHub %s asculta pe %s", version, c.Listen)
	go func() {
		if err := s.Serve(ln); err != nil && err != http.ErrServerClosed {
			log.Printf("[http] %v", err)
		}
	}()
	return nil
}

func stopHTTP() {
	srvMu.Lock()
	s := httpSrv
	httpSrv = nil
	srvMu.Unlock()
	if s != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		s.Shutdown(ctx)
		cancel()
		s.Close()
	}
}

func restartHTTP() {
	log.Printf("[http] restart server web")
	stopHTTP()
	if err := startHTTP(); err != nil {
		log.Printf("[http] EROARE la pornire pe noua adresa: %v - revin la :8090", err)
		cfgMu.Lock()
		cfg.Listen = ":8090"
		saveConfigLocked()
		cfgMu.Unlock()
		startHTTP()
	}
}

type program struct{}

func (p *program) Start(s service.Service) error {
	go p.run()
	return nil
}

func (p *program) run() {
	go hub.statsLoop()
	go clients.loop()
	applyChannels()
	go func() { // retry always-on channels that failed to start
		for range time.Tick(30 * time.Second) {
			applyChannels()
		}
	}()
	if err := startHTTP(); err != nil {
		log.Printf("[http] nu pot porni pe %s: %v", getConfig().Listen, err)
		os.Exit(1)
	}
	if service.Interactive() && runtime.GOOS == "windows" && os.Getenv("PROXYHUB_NOBROWSER") == "" {
		_, port, _ := net.SplitHostPort(getConfig().Listen)
		exec.Command("rundll32", "url.dll,FileProtocolHandler", "http://localhost:"+port+"/panel").Start()
	}
}

func (p *program) Stop(s service.Service) error {
	stopHTTP()
	return nil
}

func main() {
	defCfg := filepath.Join(exeDir(), "proxyhub.json")
	cfgFile := flag.String("c", defCfg, "fisier de configurare")
	flag.Usage = func() {
		fmt.Printf(`ProxyHub %s - udpxy + relay MPEG-TS (HTTP-TS / HLS) cu panou web

Utilizare: proxyhub [-c config.json] [comanda]

Comenzi:
  (fara)          porneste in consola
  install         instaleaza ca serviciu (Windows Service / systemd)
  uninstall       dezinstaleaza serviciul
  start|stop|restart  controleaza serviciul
  status          starea serviciului
  reset-password  reseteaza user/parola la admin/admin
`, version)
	}
	flag.Parse()
	abs, _ := filepath.Abs(*cfgFile)

	svcCfg := &service.Config{
		Name:        "ProxyHub",
		DisplayName: "ProxyHub",
		Description: "ProxyHub - udpxy + relay MPEG-TS (HTTP-TS/HLS) cu panou web",
		Arguments:   []string{"-c", abs},
		Option: service.KeyValue{
			"Restart":                "always",
			"OnFailure":              "restart",
			"OnFailureDelayDuration": "5s",
		},
	}
	if runtime.GOOS == "linux" {
		svcCfg.Dependencies = []string{"After=network-online.target", "Wants=network-online.target"}
		svcCfg.Option["LimitNOFILE"] = 65536
	}
	prg := &program{}
	svc, err := service.New(prg, svcCfg)
	if err != nil {
		fmt.Println("eroare serviciu:", err)
		os.Exit(1)
	}

	if cmd := flag.Arg(0); cmd != "" {
		switch cmd {
		case "install", "uninstall", "start", "stop", "restart":
			if cmd == "install" {
				if err := loadConfig(abs); err != nil {
					fmt.Println("config:", err)
					os.Exit(1)
				}
			}
			if err := service.Control(svc, cmd); err != nil {
				fmt.Printf("%s: %v\n", cmd, err)
				os.Exit(1)
			}
			if runtime.GOOS == "windows" {
				exe, _ := os.Executable()
				if cmd == "install" {
					exec.Command("netsh", "advfirewall", "firewall", "delete", "rule", "name=ProxyHub").Run()
					exec.Command("netsh", "advfirewall", "firewall", "add", "rule", "name=ProxyHub", "dir=in",
						"action=allow", "program="+exe, "enable=yes", "profile=any").Run()
				} else if cmd == "uninstall" {
					exec.Command("netsh", "advfirewall", "firewall", "delete", "rule", "name=ProxyHub").Run()
				}
			}
			fmt.Printf("%s: OK\n", cmd)
			return
		case "status":
			st, err := svc.Status()
			names := map[service.Status]string{service.StatusRunning: "ruleaza", service.StatusStopped: "oprit"}
			if err != nil {
				fmt.Println("status:", err)
				os.Exit(1)
			}
			fmt.Println("ProxyHub:", names[st])
			return
		case "reset-password":
			if err := loadConfig(abs); err != nil {
				fmt.Println("config:", err)
				os.Exit(1)
			}
			cfg.Username = "admin"
			cfg.PasswordHash = hashPassword("admin")
			if err := saveConfigLocked(); err != nil {
				fmt.Println(err)
				os.Exit(1)
			}
			fmt.Println("User/parola resetate la admin/admin. Reporneste serviciul.")
			return
		default:
			flag.Usage()
			os.Exit(2)
		}
	}

	if err := loadConfig(abs); err != nil {
		fmt.Println("config:", err)
		os.Exit(1)
	}
	logW := openLogFile(filepath.Join(filepath.Dir(abs), "proxyhub.log"))
	log.SetOutput(io.MultiWriter(os.Stderr, logW, logRing))
	log.SetFlags(log.Ldate | log.Ltime)
	log.Printf("ProxyHub %s (%s/%s) config: %s", version, runtime.GOOS, runtime.GOARCH, abs)

	if err := svc.Run(); err != nil {
		log.Println(err)
	}
}
