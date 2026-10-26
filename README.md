# ProxyHub

**2 în 1:** udpxy (multicast → HTTP) + relay de canale stil Astra (HTTP-TS / HLS), într-un singur executabil, cu panou web protejat cu utilizator și parolă. Rulează pe **Windows** (principal) și **Ubuntu/Linux**.

## Ce face

| Funcție | Detalii |
|---|---|
| **udpxy** | `http://IP:7000/udp/239.1.1.1:1234` și `/rtp/...` — compatibil cu playerele/playlisturile udpxy |
| **Canale** | link-uri scurte stil Astra: `http://IP:4000/<canal>` (HTTP-TS) și `http://IP:4000/<canal>.m3u8` (HLS) |
| **Surse** | `udp://@239.1.1.1:1234`, `udp://eth1@239.1.1.1:1234`, `rtp://@...`, `http(s)://...ts`, `http(s)://.../index.m3u8` |
| **PID-uri** | rămân originale — fără remux, fără transcodare |
| **Multicast** | mai multe grupuri pe același port separate corect (IP_PKTINFO), și pe Windows |
| **Panou** | login, dashboard, bitrate per canal, clienți (IP, canal, format, viteză, trafic), deconectare client, import M3U, playlist M3U, log |
| **Acces streamuri** | opțional token (`?token=`) și listă de IP-uri/CIDR permise |
| **Serviciu** | Windows Service / systemd, pornire automată |

## Instalare Windows

1. Descarcă `bin/proxyhub-windows-amd64.exe` și `bin/install-windows.bat` în același folder (ex. `C:\ProxyHub`).
2. Dublu-click pe `install-windows.bat` (cere drepturi de admin) → instalează serviciul, regula de firewall și deschide panoul.
3. Panou: **http://localhost:4000/panel** — login **admin / admin** (schimbă-l din *Setări*).

Doar test, fără serviciu: dublu-click pe `proxyhub-windows-amd64.exe` (rulează în consolă și deschide browserul).

## Instalare Ubuntu

```bash
curl -fsSL https://raw.githubusercontent.com/maxvideostar/ProxyHub/main/install.sh | sudo bash
```

Instalează în `/opt/proxyhub`, creează serviciul systemd `ProxyHub`, deschide portul în ufw. Aceeași comandă face și actualizarea.

```bash
systemctl status ProxyHub
journalctl -u ProxyHub -f
```

## Comenzi

```
proxyhub [-c proxyhub.json] [comanda]
  (fără)          rulează în consolă
  install         instalează serviciul
  uninstall       dezinstalează serviciul
  start | stop | restart | status
  reset-password  resetează login la admin/admin
```

Configurația (`proxyhub.json`) și logul (`proxyhub.log`) stau lângă executabil.

## Porturi

Implicit: **4000** = relay canale + panou, **7000** = udpxy (panoul e accesibil și pe 7000). Se schimbă din *Setări* → „Repornește serverul web”, sau din `proxyhub.json` (`listen`, `udpxy_listen`) + restart serviciu. `udpxy_listen` gol = udpxy pe același port cu relay-ul.

## Link-uri

```
http://IP:7000/udp/239.1.1.1:1234        udpxy (UDP)
http://IP:7000/rtp/239.1.1.1:5000        udpxy (RTP)
http://IP:4000/hd365                     canal HTTP-TS
http://IP:4000/hd365.m3u8                canal HLS
http://IP:4000/playlist.m3u              playlist toate canalele (TS)
http://IP:4000/playlist.m3u?type=hls     playlist HLS
```

Cu token setat se adaugă `?token=XXX` (playlistul îl include automat).

## Build din sursă

Go 1.24+:

```bash
./build.sh     # -> bin/proxyhub-windows-amd64.exe, proxyhub-linux-amd64, proxyhub-linux-arm64
```
