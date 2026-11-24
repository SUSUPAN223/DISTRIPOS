# Panduan Operasional DistriPOS v2.0

Panduan ini mencakup persiapan, konfigurasi, menjalankan klaster, operasi melalui CLI dan Web Dashboard, pengujian fitur satu per satu, serta shutdown. Contoh perintah menggunakan Windows PowerShell.

> **Perhatian data:** transaksi, transfer, seed, penghapusan SKU, dan sinkronisasi mengubah data di `data/`. Gunakan katalog demo atau SKU uji unik. Seed bukan undo. Backup `data/` sebelum demonstrasi yang mengubah data.

## 1. Prasyarat dan instalasi

- Python 3.10 atau lebih baru dan dependency pada `requirements.txt`.
- Port tersedia: XML-RPC `5001–5004`, Pyro5 RMI `6001–6004`, launcher/dashboard `8000`, REST `8001–8004`.
- MPI opsional. Tanpa runtime MPI dan `mpi4py` yang cocok, audit memakai simulasi empat rank.

Buka PowerShell di root proyek:

```powershell
cd "D:\Kuliah\Code\Matkul\Sistem Terdistribusi\distributed-branch-inventory"
python -m venv .venv
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Jika virtualenv sudah tersedia, cukup aktifkan. Buat `.env` hanya jika belum ada:

```powershell
Copy-Item .env.example .env
```

Jangan menimpa `.env` yang sudah dikonfigurasi. File tersebut bersifat lokal dan jangan dikomit.

## 2. Konfigurasi `.env`

Contoh satu komputer:

```dotenv
BIND_HOST=0.0.0.0
HOST_NODE1=localhost
HOST_NODE2=localhost
HOST_NODE3=localhost
HOST_NODE4=localhost
LAUNCHER_HOST=0.0.0.0
LAUNCHER_PORT=8000
RPC_BASE_PORT=5000
RMI_BASE_PORT=6000
API_BASE_PORT=8000
CLI_HTTP_TIMEOUT=3
```

- `BIND_HOST` adalah interface bind REST/XML-RPC, bukan host tujuan.
- `HOST_NODE1..4` adalah alamat routable tujuan/iklan node. Satu PC gunakan `localhost`; multi-PC gunakan IP LAN atau DNS mesin node.
- `RMI_BIND_HOST_NODEn` opsional untuk override interface lokal Pyro.
- `LAUNCHER_HOST`/`LAUNCHER_PORT` mengatur launcher. Launcher dapat mengendalikan proses; batasi aksesnya ke jaringan tepercaya.
- Port default akhir: REST `8001–8004`, XML-RPC `5001–5004`, RMI `6001–6004`.

### Mode multi-PC

Pada setiap mesin, isi pemetaan `HOST_NODE1..4` yang sama dengan alamat routable tiap node. Set `BIND_HOST=0.0.0.0` pada mesin node yang perlu menerima koneksi jaringan. Buka firewall secara terbatas untuk TCP `5001–5004`, `6001–6004`, `8001–8004`, dan `8000` pada mesin launcher. Browser juga harus dapat mengakses setiap host REST; `localhost` pada browser menunjuk PC pengguna browser, bukan server.

Jalankan satu `python run_node.py --node N --host 0.0.0.0` di mesin masing-masing node. Jalankan launcher hanya pada mesin dashboard/koordinator **tanpa** `--auto-start`, karena launcher hanya memulai proses lokal. Uji alamat Pyro yang diiklankan, routing, firewall, dan CORS di jaringan sebenarnya. Jangan buka launcher ke Internet atau jaringan tak tepercaya.

## 3. Menghidupkan dan mematikan klaster

### Satu komputer (disarankan)

Di terminal pertama, dari root proyek dan dengan `.venv` aktif:

```powershell
python launcher.py --auto-start
```

Biarkan terminal tetap terbuka. Launcher menjalankan Node 1–4. Buka Dashboard:

```text
http://localhost:8000/
```

Periksa status dari terminal kedua:

```powershell
Invoke-RestMethod http://localhost:8000/launcher/status | ConvertTo-Json -Depth 5
```

Tunggu sampai Node 1–4 `running` dan indikator Dashboard online. Heartbeat/recovery dapat memerlukan beberapa detik.

Hentikan dengan `Ctrl+C` pada terminal launcher atau:

```powershell
Invoke-RestMethod -Method Post http://localhost:8000/launcher/stop_all
```

Node yang dijalankan manual harus dihentikan dengan `Ctrl+C` pada terminal node; launcher tidak mengelola PID tersebut. Untuk memastikan port kosong:

```powershell
$ports = 5001..5004 + 6001..6004 + 8000..8004
Get-NetTCPConnection -State Listen -ErrorAction SilentlyContinue |
  Where-Object { $ports -contains $_.LocalPort } |
  Select-Object LocalAddress, LocalPort, OwningProcess
```

### Node manual

Jalankan satu perintah per terminal (atau node yang sesuai pada masing-masing PC):

```powershell
python run_node.py --node 1 --host 0.0.0.0
python run_node.py --node 2 --host 0.0.0.0
python run_node.py --node 3 --host 0.0.0.0
python run_node.py --node 4 --host 0.0.0.0
```

## 4. Aturan operasi dan trace

- Node 1–3 adalah node primer. Node 4 replika/auditor dan bukan tujuan transaksi kasir.
- Stok integer int32; harga dan `min_stok` integer nonnegatif. Jumlah negatif mengurangi stok, positif menambah. Stok tidak boleh negatif.
- Mutasi menaikkan versi dan menambah riwayat. Push replikasi asynchronous, sehingga versi bisa berbeda sesaat.
- Respons umumnya memuat `ok`, `data` atau `error`, serta `trace`: `dari`, `ke`, `protokol`, `aksi`, `ringkasan_data`, `status`.
- Smart Gateway pindah gateway hanya saat koneksi gagal/timeout. Respons HTTP 4xx/5xx, termasuk 502 rollback, tidak diulang.
- Bila mutasi timeout, periksa stok dan versi sebelum mencoba lagi karena operasi mungkin sudah diterapkan.

## 5. Menggunakan CLI

Di terminal kedua:

```powershell
cd "D:\Kuliah\Code\Matkul\Sistem Terdistribusi\distributed-branch-inventory"
.\.venv\Scripts\Activate.ps1
python cli.py
```

Menu interaktif menyediakan daftar/cari/tambah SKU, mutasi stok, hapus SKU, transfer, DHT, MPI, status, lock/replika, seed, sinkronisasi, dan delay. Ikuti prompt; masukkan jumlah negatif untuk penjualan. Tekan `0` untuk keluar dari CLI tanpa mematikan launcher.

### Perintah langsung

```powershell
# Baca inventori
python cli.py list --node 2
python cli.py get BRG001 --node 2

# Buat SKU baru yang unik di Node 2
python cli.py add DEMO-CLI-001 "Barang Demo CLI" 20 --harga 12000 --min-stok 3 --node 2

# Jual 2 unit, lalu restock 5 unit
python cli.py stock DEMO-CLI-001 -2 --tipe penjualan --node 2
python cli.py stock DEMO-CLI-001 5 --tipe restock --node 2

# Hapus hanya SKU uji setelah selesai
python cli.py delete DEMO-CLI-001 --node 2

# Transfer normal dan simulasi rollback (SKU harus ada dan stok cukup di Node 2)
python cli.py transfer 2 3 BRG001 1
python cli.py transfer 2 3 BRG001 1 --simulasi-gagal

# DHT, status, lock, replika, sinkronisasi, MPI
python cli.py dht BRG001
python cli.py status
python cli.py lock
python cli.py replica
python cli.py sync
python cli.py mpi

# Fault injection: reset kembali ke 0 setelah pengujian
python cli.py delay 2 4
python cli.py delay 2 0
```

Transfer hanya untuk cabang 2 dan 3 yang berbeda; jumlah harus positif. Simulasi gagal seharusnya menampilkan trace `ROLLBACK`. `delay 2 4` dapat menyebabkan timeout, jadi reset segera dengan nilai `0`. `python cli.py seed` mengubah data dan hanya dijalankan bila memang ingin memasang data awal.

Opsi lengkap:

```powershell
python cli.py --help
python cli.py add --help
python cli.py transfer --help
```

## 6. Menggunakan Web Dashboard

Buka `http://localhost:8000/` dan tunggu indikator N1–N4 online.

### Workspace

- **Kasir & Stok → Transaksi stok:** pilih Node 1–3 dan SKU. Masukkan jumlah lalu **Jual** (negatif) atau **Restock** (positif). Tombol cepat hanya mengisi jumlah.
- **Kasir & Stok → Kelola SKU:** masukkan kode, nama, stok awal, harga, dan minimum stok. Kode duplikat ditolak. Penghapusan memerlukan konfirmasi dan bersifat destruktif pada node terpilih.
- **Transfer:** pilih asal/tujuan Node 2 dan 3, SKU yang ada di sumber, serta jumlah positif. SKU yang belum ada di tujuan dibuat otomatis dengan metadata dari sumber.
- **Simulasi Gagal / Rollback:** aktifkan sebelum transfer uji. Periksa error, trace `ROLLBACK`, release lock, dan stok sumber yang dipulihkan.
- **Cari DHT:** masukkan kode untuk melihat node pemilik, rute finger hop, dan stok bila tersedia.

### Topologi dan Cluster Tools

- Klik kartu node untuk membuka daftar barang lengkap; topologi juga menunjukkan status, versi, dan lock.
- **Log & Lineage:** telusuri trace dan filter `XML-RPC`, `Pyro5-RMI`, `FAILOVER`, `ROLLBACK`, `CHORD-DHT`, `MPI`. Bersihkan hanya menghapus log browser.
- **Chaos & Node:** hidupkan/matikan node yang dikelola launcher dan gunakan delay untuk fault injection. Jangan matikan Node 4 saat menguji replika atau LockManager cadangan.
- **Replika & Info:** bandingkan versi sumber/replika dan gunakan sinkron ulang jika sumber hidup.
- **Seed Data:** menyiapkan katalog awal, bukan undo. Hindari jika ingin menjaga data saat ini.
- **Audit MPI:** periksa valuasi, stok kritis, discrepancy, SKU prioritas dan label mode. Fallback simulasi bukan MPI runtime nyata.

## 7. Checklist uji fitur secara berurutan

Gunakan data demo atau SKU unik dan stok yang cukup. Catat stok awal; jangan gunakan barang operasional untuk skenario destruktif.

### A. Seed, baca, dan replika

1. Jika membutuhkan katalog demo, tekan **Seed Data** atau `python cli.py seed` (operasi ini mengubah data).
2. Jalankan `python cli.py status`, lalu `python cli.py list --node 1`, `--node 2`, dan `--node 3`.
3. Periksa **Replika & Info** atau `python cli.py replica`; tunggu push asynchronous. Jalankan `python cli.py sync` untuk sinkron ulang.
4. Periksa `data/data_nodeN.json`: `versi_global` ada di root, barang memiliki `harga`/`min_stok`, dan mutasi masuk `riwayat_mutasi`.

### B. CRUD dan kasir

1. Tambah `DEMO-2026-01` pada Node 2 melalui CLI `add` atau Dashboard Kelola SKU.
2. Baca dengan `get`/`list` dan lihat pada Dashboard.
3. Jual 1–2 unit lalu restock; periksa stok, versi, dan trace.
4. Coba menjual melebihi stok: operasi harus ditolak dan stok tidak negatif.
5. Hapus hanya SKU uji setelah tidak diperlukan skenario selanjutnya.

### C. Transfer normal, auto-create, rollback

1. Pastikan SKU ada di Node 2 dengan stok cukup; transfer satu unit ke Node 3 secara normal.
2. Untuk auto-create, gunakan SKU yang ada di sumber tetapi belum ada di tujuan; setelah transfer periksa SKU, metadata, dan stok tujuan.
3. Jalankan transfer lagi dengan `--simulasi-gagal` atau switch Dashboard.
4. Verifikasi respons gagal (umumnya HTTP 502), trace `ROLLBACK` dan release lock, serta stok sumber sama dengan sebelum percobaan.
5. Jangan retry HTTP 502: gateway memang tidak memutar ulang request.

### D. Chord DHT dan MPI

1. Jalankan `python cli.py dht KODE` atau tab **Cari DHT**; amati pemilik, hop, dan stok.
2. Cari kode yang tidak ada; lookup tidak boleh membuat SKU.
3. Jalankan `python cli.py mpi` atau **Audit MPI**; periksa hasil dan trace `MPI`.
4. Bedakan mode runtime dari fallback simulasi. MPI nyata memerlukan runtime MPI dan `mpi4py` yang kompatibel.

### E. Delay, read failover, dan recovery

1. Atur `python cli.py delay 2 4` atau tombol delay Dashboard; uji polling/operasi yang sesuai.
2. Reset wajib dengan `python cli.py delay 2 0`.
3. Pastikan Node 4 punya snapshot Node 2. Matikan Node 2 melalui Chaos & Node atau:

   ```powershell
   Invoke-RestMethod -Method Post http://localhost:8000/launcher/stop/2
   ```

4. Baca Node 2 melalui `python cli.py list --node 2` atau Dashboard. Periksa trace failover ke Node 4.
5. Hidupkan Node 2:

   ```powershell
   Invoke-RestMethod -Method Post http://localhost:8000/launcher/start/2
   ```

6. Tunggu heartbeat/recovery; jalankan `python cli.py sync` atau **Paksa Sinkron Ulang** dan periksa versi replika.

### F. LockManager failover

1. Pastikan Node 4 hidup, lalu matikan Node 1 melalui Chaos & Node atau:

   ```powershell
   Invoke-RestMethod -Method Post http://localhost:8000/launcher/stop/1
   ```

2. Jalankan transfer Node 2→3 dengan stok cukup. Jika gateway pilihan mati, Smart Gateway dapat berpindah saat koneksi gagal. Untuk mengarahkan permintaan langsung ke cabang, gunakan `http://localhost:8002/transfer`.
3. Periksa trace `FAILOVER` ke Node 4 dan `acquire_lock_cadangan`.
4. Hidupkan Node 1 kembali dan tunggu status sehat.

### G. Smart Gateway failover

Dashboard tetap disajikan launcher pada port 8000. Gateway REST berpindah saat koneksi gagal; read failover data adalah mekanisme berbeda yang membaca snapshot Node 4. Respons HTTP 4xx/5xx, termasuk rollback 502, tidak diulang.

## 8. Endpoint pemeriksaan

```powershell
Invoke-RestMethod http://localhost:8000/launcher/status | ConvertTo-Json -Depth 5
Invoke-RestMethod http://localhost:8001/status | ConvertTo-Json -Depth 6
Invoke-RestMethod http://localhost:8001/lock | ConvertTo-Json -Depth 5
Invoke-RestMethod http://localhost:8001/replika | ConvertTo-Json -Depth 6
Invoke-RestMethod http://localhost:8001/health | ConvertTo-Json -Depth 4
```

Endpoint lain: `GET /dht/lookup/{kode}`, `POST /analytics/mpi` dengan body `{}`, dan `POST /admin/delay/{n}` dengan body `{"detik":0}` untuk reset delay.

## 9. Troubleshooting

| Gejala | Tindakan |
|---|---|
| Port terpakai | Periksa `Get-NetTCPConnection`; hentikan proses lama yang benar sebelum start ulang. |
| Node belum online | Tunggu beberapa detik; periksa `.env`, port, terminal/log, dan proses duplikat. |
| CLI tidak terhubung | Jalankan dari root project dengan `.venv` aktif; cek node hidup dan `HOST_NODE*`. |
| Transfer ditolak | Pastikan asal/tujuan 2 dan 3 berbeda, SKU ada di sumber, jumlah positif, stok cukup. |
| Replika stale | Tunggu push asynchronous; bila sumber hidup, jalankan sinkron ulang. |
| Dashboard multi-PC gagal | Periksa alamat API yang routable dari browser, firewall, dan CORS; `localhost` menunjuk mesin browser. |
| MPI fallback | Runtime MPI/mpi4py tidak tersedia atau gagal; fallback hanya simulasi. |
| Mutasi timeout | Periksa stok/versi dahulu, jangan langsung mencoba ulang. |

Panduan terkait: [Menjalankan](PANDUAN_MENJALANKAN.md), [Fitur Dashboard](PANDUAN_FITUR_DASHBOARD.md), dan [Spesifikasi Teknis](SPESIFIKASI_TEKNIS.md).