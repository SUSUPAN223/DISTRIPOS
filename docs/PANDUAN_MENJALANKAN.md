# Panduan Menjalankan DistriPOS

Panduan ini menjalankan seluruh klaster lokal empat node, membuka dashboard, menjalankan MPI, serta menguji endpoint REST. Perintah utama ditulis untuk PowerShell Windows. Perintah `curl.exe` dapat digunakan dari PowerShell.

## 1. Prasyarat

- Python 3.10 atau lebih baru.
- Port lokal `5001–5004` (XML-RPC), `6001–6004` (Pyro5 RMI), `8000` (launcher), dan `8001–8004` (REST node) tersedia.
- MPI opsional. Audit memakai simulasi empat rank jika MPI sistem tidak tersedia.
- Untuk MPI nyata, pasang runtime MPI yang sesuai dengan OS (Microsoft MPI, Open MPI, atau MPICH), pastikan `mpiexec` tersedia di `PATH`, dan pasang `mpi4py` yang cocok dengan runtime tersebut.

## 2. Buka project dan pasang dependency

```powershell
cd "D:\Kuliah\Code\Matkul\Sistem Terdistribusi\distributed-branch-inventory"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
```

Jika aktivasi ditolak PowerShell, izinkan script untuk sesi saat ini lalu aktifkan ulang:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy Bypass
.\.venv\Scripts\Activate.ps1
```

Buat konfigurasi lokal dari template:

```powershell
Copy-Item .env.example .env
```

Nilai bawaan memakai `127.0.0.1`, cocok untuk satu komputer. Port default:

| Layanan | Port |
|---|---:|
| Process Launcher dan dashboard | `8000` |
| REST API Node 1–4 | `8001–8004` |
| XML-RPC Node 1–4 | `5001–5004` |
| Pyro5 RMI Node 1–4 | `6001–6004` |

Untuk beberapa komputer, sesuaikan `HOST_NODE1` sampai `HOST_NODE4` di `.env` dan pastikan port tersebut dibuka di firewall.

Pemeriksaan sintaks opsional:

```powershell
python -m compileall -q src run_node.py launcher.py
```

## 3. Jalankan sistem melalui Process Launcher (disarankan)

Di terminal project dengan environment aktif, jalankan:

```powershell
python launcher.py --auto-start
```

Biarkan terminal tersebut tetap terbuka. Launcher di `http://127.0.0.1:8000` akan menyalakan Node 1–4. Buka dashboard di browser:

```text
http://127.0.0.1:8000/
```

Periksa status proses dari terminal PowerShell kedua:

```powershell
curl.exe http://127.0.0.1:8000/launcher/status
```

Node mungkin berstatus `starting` sejenak saat proses baru dinyalakan.

Launcher menyediakan operasi berikut:

```powershell
curl.exe -X POST http://127.0.0.1:8000/launcher/start/1
curl.exe -X POST http://127.0.0.1:8000/launcher/stop/1
curl.exe -X POST http://127.0.0.1:8000/launcher/start_all
curl.exe -X POST http://127.0.0.1:8000/launcher/stop_all
```

## 4. Alternatif: jalankan node manual

Buka empat terminal PowerShell di folder project. Aktifkan environment pada masing-masing terminal, lalu jalankan satu perintah berikut per terminal:

```powershell
python run_node.py --node 1
```

```powershell
python run_node.py --node 2
```

```powershell
python run_node.py --node 3
```

```powershell
python run_node.py --node 4
```

Hentikan masing-masing node dengan `Ctrl+C`. Jika proses dijalankan manual, launcher tidak mengelola PID-nya; `/launcher/status` hanya menunjukkan proses yang dimulai oleh launcher.

## 5. Seed data awal

Seed Node 1–3 dan minta Node 4 menyinkronkan snapshot:

```powershell
curl.exe -X POST http://127.0.0.1:8001/admin/seed `
  -H "Content-Type: application/json" `
  -d "{}"
```

Pastikan Node 1–4 sudah hidup sebelum mengharapkan seluruh sumber replika tersedia. Dashboard juga menyediakan tombol **Reset / Seed Data Awal**.

## 6. Uji REST API

Respons Node API umumnya memuat `ok`, `via`, `node_tujuan`, dan `trace`. Parameter `node=N` memilih node target untuk operasi barang.

### Baca daftar barang dan total stok

```powershell
curl.exe "http://127.0.0.1:8001/barang?node=2"
curl.exe "http://127.0.0.1:8001/total?node=2"
```

### Tambah SKU

```powershell
curl.exe -X POST "http://127.0.0.1:8002/barang?node=2" `
  -H "Content-Type: application/json" `
  -d "{`"kode`":`"SKU-TEST`",`"nama`":`"Barang Uji`",`"stok`":20,`"harga`":10000,`"min_stok`":3}"
```

Berhasil mengembalikan HTTP `201`; SKU duplikat mengembalikan HTTP `409`.

### Transaksi stok kasir

Jumlah negatif mengurangi stok, jumlah positif menambah stok:

```powershell
curl.exe -X PUT "http://127.0.0.1:8002/barang/SKU-TEST/stok?node=2" `
  -H "Content-Type: application/json" `
  -d "{`"jumlah`":-3,`"tipe`":`"penjualan`",`"request_id`":`"sale-001`"}"
```

Stok yang tidak mencukupi menghasilkan HTTP `422`.

### Hapus SKU

```powershell
curl.exe -X DELETE "http://127.0.0.1:8002/barang/SKU-TEST?node=2"
```

### Transfer antar cabang: sukses dan rollback

Transfer normal Node 2 ke Node 3:

```powershell
curl.exe -X POST http://127.0.0.1:8001/transfer `
  -H "Content-Type: application/json" `
  -d "{`"dari`":2,`"ke`":3,`"kode`":`"SKU-TEST`",`"jumlah`":2,`"simulasi_gagal`":false}"
```

Untuk menguji kegagalan cabang tujuan dan auto-rollback:

```powershell
curl.exe -X POST http://127.0.0.1:8001/transfer `
  -H "Content-Type: application/json" `
  -d "{`"dari`":2,`"ke`":3,`"kode`":`"SKU-TEST`",`"jumlah`":2,`"simulasi_gagal`":true}"
```

Trace kegagalan menunjukkan lock, debit, rollback, dan pelepasan lock. Dashboard menampilkan langkah-langkah tersebut sebagai animasi lineage.

### Read failover saat node target mati

Pastikan replika sudah berisi snapshot, lalu matikan Node 2 lewat launcher:

```powershell
curl.exe -X POST http://127.0.0.1:8000/launcher/stop/2
```

Minta data Node 2 melalui gateway Node 1:

```powershell
curl.exe "http://127.0.0.1:8001/barang/SKU-TEST?node=2"
```

Hasil fallback berasal dari snapshot Node 4 dan dapat lebih lama daripada data primer. Hidupkan kembali Node 2 untuk menguji recovery:

```powershell
curl.exe -X POST http://127.0.0.1:8000/launcher/start/2
```

Dashboard memilih gateway REST lain (`8002–8004`) jika gateway utama (`8001`) tidak dapat diakses. Perlu dibedakan: penggantian **gateway** tidak menghidupkan kembali node target yang mati; fallback isi data tetap bergantung pada Read Failover backend dan snapshot replika Node 4.

### Delay, status, lock, replika, dan Chord DHT

```powershell
# Heartbeat semua node, versi global, AVTree
curl.exe http://127.0.0.1:8001/status

# Lock aktif dan sisa lease
curl.exe http://127.0.0.1:8001/lock

# Ringkasan versi snapshot Node 4
curl.exe http://127.0.0.1:8001/replika

# Sinkron ulang snapshot Node 4 dari Node 1–3
curl.exe -X POST http://127.0.0.1:8001/replika/sinkron_ulang `
  -H "Content-Type: application/json" -d "{}"

# Cari pemilik dan rute SKU melalui Chord DHT
curl.exe http://127.0.0.1:8001/dht/lookup/SKU-TEST

# Suntik delay empat detik ke Node 2
curl.exe -X POST http://127.0.0.1:8001/admin/delay/2 `
  -H "Content-Type: application/json" `
  -d "{`"detik`":4.0}"
```

Reset delay setelah demo:

```powershell
curl.exe -X POST http://127.0.0.1:8001/admin/delay/2 `
  -H "Content-Type: application/json" `
  -d "{`"detik`":0}"
```

### Audit MPI melalui REST

```powershell
curl.exe -X POST http://127.0.0.1:8001/analytics/mpi `
  -H "Content-Type: application/json" -d "{}"
```

Respons memuat agregat stok dan valuasi, stok kritis, discrepancy vector, serta mode audit. Tombol **Jalankan Audit Paralel MPI** di dashboard membuka ringkasan hasilnya.

## 7. Menjalankan MPI langsung

MPI nyata memerlukan `mpiexec` dan `mpi4py` yang kompatibel:

```powershell
mpiexec -n 4 python -m src.analytics.mpi_analytics
```

Fallback simulasi empat rank tidak memerlukan runtime MPI:

```powershell
python -m src.analytics.mpi_analytics --simulate
```

Kedua mode mencetak JSON ke stdout. Endpoint REST mencoba mode nyata bila tersedia, lalu memakai simulasi jika executable atau runtime MPI gagal.

## 8. Menghentikan klaster

Untuk menghentikan node yang dikelola launcher, gunakan:

```powershell
curl.exe -X POST http://127.0.0.1:8000/launcher/stop_all
```

Atau tekan `Ctrl+C` pada terminal launcher; launcher akan mencoba menghentikan proses anak. Untuk mode manual, tekan `Ctrl+C` pada masing-masing terminal node.

## 9. Troubleshooting

- **Address already in use:** port sudah dipakai proses lama. Hentikan proses tersebut sebelum memulai cluster kembali.
- **Node berstatus `starting` atau offline:** tunggu startup selesai, periksa `/launcher/status`, dan lihat terminal proses launcher/node.
- **Dashboard tidak dapat menghubungi launcher:** pastikan `python launcher.py` berjalan dan buka dashboard dari `http://127.0.0.1:8000/`.
- **Sinkronisasi replika gagal sebagian:** aktifkan Node 1–3, lalu panggil ulang `/replika/sinkron_ulang`.
- **Transfer gagal:** pastikan SKU ada dan stok cabang sumber mencukupi. `simulasi_gagal: true` memang memaksa rollback.
- **MPI berjalan sebagai fallback:** instal runtime MPI sistem dan `mpi4py` yang sesuai; fallback tetap mendukung demo audit.
- **Konfigurasi port berbeda:** periksa nilai `API_BASE_PORT`, `RPC_BASE_PORT`, `RMI_BASE_PORT`, serta pengaturan launcher di `.env`.