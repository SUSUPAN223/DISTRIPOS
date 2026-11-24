# Catatan Penggunaan Fitur Dashboard DistriPOS

Panduan ini menjelaskan penggunaan dashboard DistriPOS v2.0 dengan benar, termasuk urutan operasi yang disarankan, arti status, dan batasan yang perlu diketahui saat demo. Untuk instalasi dependency dan menjalankan server pertama kali, lihat [`PANDUAN_MENJALANKAN.md`](PANDUAN_MENJALANKAN.md).

## 1. Mulai dari kondisi klaster yang sehat

1. Jalankan Process Launcher dan seluruh node, misalnya:

   ```powershell
   python launcher.py --auto-start
   ```

2. Buka `http://127.0.0.1:8000/`.
3. Tunggu indikator N1–N4 pada header berubah menjadi **ONLINE**. Node yang baru start dapat memerlukan beberapa detik untuk inisialisasi RPC, RMI, heartbeat, dan recovery.
4. Untuk data demo yang konsisten, tekan **Seed Data** sekali setelah Node 1–4 hidup.
5. Tunggu notifikasi seed dan polling berikutnya. Node 4 harus memiliki snapshot sumber Node 1–3.

Dashboard memperbarui status dan inventori secara otomatis setiap 2,5 detik. Polling memperbarui tampilan tanpa menghentikan antrean animasi lineage yang sedang diputar.

## 2. Memahami layout

- **Header**: status empat node, tombol **Seed Data**, dan **Audit MPI**.
- **Panel kiri — Workspace**: tab **Kasir & Stok**, **Transfer**, dan **Cari DHT**. Tab Kasir & Stok memiliki sub-tab **Transaksi stok** dan **Kelola SKU**.
- **Kanvas tengah — Topologi jaringan**: kartu Node 1–4, versi, beberapa SKU, lock aktif, jalur komunikasi, dan animasi trace. Klik kartu node untuk membuka daftar barang lengkap.
- **Panel kanan — Cluster tools**: tab **Log & Lineage**, **Chaos & Node**, dan **Replika & Info**.
- **Toast**: notifikasi singkat keberhasilan, kegagalan, gateway failover, atau rollback muncul di pojok kanan atas.

Nomor node dan peran:

| Node | Peran | REST | XML-RPC | RMI |
|---|---|---:|---:|---:|
| 1 | Pusat / coordinator | 8001 | 5001 | 6001 |
| 2 | Cabang A | 8002 | 5002 | 6002 |
| 3 | Cabang B | 8003 | 5003 | 6003 |
| 4 | Replika / auditor | 8004 | 5004 | 6004 |

## 3. Menyiapkan atau mengatur ulang data demo

1. Pastikan node yang ingin digunakan hidup.
2. Klik **Seed Data**.
3. Perhatikan toast. Bila proses berhasil, backend menjalankan seed pada node aktif dan memicu sinkron ulang replika.
4. Periksa versi dan status replika di kartu Node dan tab **Replika & Info**.

Seed menyiapkan data demo, bukan tombol undo untuk mengembalikan seluruh aktivitas ke keadaan sebelumnya. Jalankan sebelum demonstrasi atau saat ingin memasukkan katalog awal lagi. Perubahan setelah seed dapat mengubah stok dan versi.

## 4. Kasir & Stok

### 4.1 Transaksi stok: penjualan atau restock

1. Pilih tab **Kasir & Stok** lalu sub-tab **Transaksi stok**.
2. Pilih **Node tujuan** tempat barang akan dimutasi. Node 1–3 menyimpan inventori primer; Node 4 adalah replika dan terutama untuk failover/bacaan.
3. Pilih SKU dari saran yang muncul atau ketik kodenya. Saran SKU berasal dari katalog node terpilih.
4. Masukkan jumlah, gunakan tombol cepat, atau tekan tombol operasi:
   - **Jual (−)** mengubah jumlah menjadi negatif. Jika input kosong, nilai default adalah `-1`.
   - **Restock (+)** mengubah jumlah menjadi positif. Jika input kosong, nilai default adalah `+10`.
   - Tombol cepat `−1`, `−5`, dan `+10` hanya mengisi jumlah; mutasi belum dikirim sampai tombol Jual/Restock ditekan.
5. Periksa kode SKU, node, dan jumlah, lalu klik **Jual** atau **Restock**.
6. Tunggu toast dan animasi trace. Setelah mutasi sukses, backend menaikkan versi lokal dan mengirim push replication asynchronous ke Node 4.

Aturan penting:

- SKU harus sudah ada di node yang dipilih. Jika belum, buat lewat **Kelola SKU**.
- Stok tidak boleh menjadi negatif; penjualan berlebih ditolak backend (HTTP 422).
- Mutasi stok berlaku pada node yang dipilih. Jangan menganggap transaksi lokal di satu cabang otomatis mengubah stok cabang lain.
- Node 4 adalah replika. Untuk transaksi kasir, gunakan Node 1, 2, atau 3.

### 4.2 Menambah SKU

1. Pilih sub-tab **Kelola SKU**.
2. Isi **Kode SKU**, **Nama barang**, dan **Stok awal**.
3. Harga opsional: bila dikosongkan, dashboard mengirim default `50000`.
4. Min. stok opsional: bila dikosongkan, dashboard mengirim default `15`.
5. Pastikan **Node tujuan** benar, lalu klik **Tambah SKU Baru**.
6. SKU duplikat pada node tujuan ditolak; gunakan kode berbeda.

Gunakan kode SKU yang konsisten untuk transaksi, transfer, pencarian DHT, dan penghapusan.

### 4.3 Menghapus SKU

1. Di sub-tab **Kelola SKU**, pilih **Node tujuan** yang benar.
2. Isi **Kode SKU yang akan dihapus**. Setelah penambahan SKU, field ini otomatis diisi dengan kode yang baru dibuat.
3. Klik **Hapus SKU** dan konfirmasi dialog browser.
4. Periksa toast dan lineage.

Penghapusan destruktif pada node yang dipilih. Pastikan kodenya benar dan sinkronkan replika bila ingin memperbarui snapshot.

## 5. Transfer stok antar cabang

1. Buka tab **Transfer**.
2. Pilih **Asal** dan **Tujuan**. Hanya Node 2 dan Node 3 yang valid; keduanya harus berbeda.
3. Isi SKU yang ada di cabang asal serta jumlah positif.
4. Untuk transfer normal, biarkan switch **Simulasi Gagal / Rollback** mati.
5. Klik **Jalankan Transfer**.
6. Ikuti animasi dan log: akuisisi distributed lock, pengurangan stok asal, pengiriman ke cabang tujuan, dan pelepasan lock.

### Demo rollback

1. Pastikan SKU ada dan stok cabang asal cukup.
2. Aktifkan **Simulasi Gagal / Rollback**.
3. Jalankan transfer.
4. Kegagalan tujuan memicu kompensasi: stok asal ditambahkan kembali dan lock dilepas.
5. Periksa toast serta event `ROLLBACK` dan `release_lock` di Log & Lineage.

Transfer memakai lock lease dan kompensasi, bukan transaksi database atomik lintas node. Jika proses terganggu, periksa stok kedua cabang sebelum mencoba ulang.

## 6. Pencarian katalog Chord DHT

1. Buka tab **Cari DHT**.
2. Masukkan kode SKU lalu klik **Cari rute**.
3. Kotak hasil menampilkan node pemilik, jumlah/rute finger hop, dan stok aktual bila item ditemukan.
4. Kanvas memutar trace `CHORD-DHT`; pembacaan aktual dapat menambahkan langkah `XML-RPC` atau failover replika.

Chord lookup menunjukkan pemilik berdasarkan pemetaan/routing DHT. Bila SKU tidak tersedia di pemilik, stok akan ditampilkan sebagai tidak ditemukan. DHT bukan pencarian fuzzy berdasarkan nama.

## 7. Membaca kanvas topologi

- **ONLINE/OFFLINE**: status node dari heartbeat dan/atau proses yang dikelola launcher.
- **Versi Global**: versi dokumen inventori terakhir yang terbaca. Node 4 menampilkan versi snapshot tiap sumber, bukan satu versi global tunggal.
- **SKU Teratas**: tiga SKU pertama berdasarkan urutan kode, bukan ranking stok.
- **LOCKED**: muncul saat backend melaporkan lock aktif; angka lease berasal dari status lock.
- **Klik kartu node**: membuka seluruh daftar barang node itu. Tutup dengan tombol `×` atau Escape.
- **Beam dan tooltip**: warna menunjukkan protokol; tooltip memuat aksi dan ringkasan payload. Langkah trace diputar berurutan.

Warna animasi: XML-RPC cyan, Pyro5-RMI ungu, FAILOVER amber, ROLLBACK merah, serta CHORD-DHT/MPI hijau.

## 8. Log & Lineage

1. Buka tab kanan **Log & Lineage**.
2. Jalankan transaksi, transfer, DHT lookup, sinkronisasi, atau audit.
3. Setiap event menampilkan waktu, rute Node asal → tujuan, protokol, aksi, ringkasan, dan status.
4. Gunakan dropdown protokol untuk memfilter `XML-RPC`, `Pyro5-RMI`, `FAILOVER`, `ROLLBACK`, `CHORD-DHT`, atau `MPI`.
5. Klik **Bersihkan** untuk mengosongkan log browser. Ini tidak menghapus data inventori atau log server.

Log browser menyimpan maksimum 250 event terbaru dan tidak dipersistenkan setelah reload. Tunggu animasi selesai untuk membaca urutan trace lengkap.

## 9. Chaos engineering dan kontrol node

1. Buka tab kanan **Chaos & Node**.
2. Tombol tunggal tiap node berganti berdasarkan status: **Matikan** menghentikan proses lewat launcher; **Hidupkan** menjalankannya kembali.
3. Klik **Delay 4s** untuk memasang delay RPC pada node itu. Delay dapat melewati timeout dan membantu demo fault handling.
4. Tunggu polling dan heartbeat untuk melihat perubahan status.
5. Setelah demo, reset delay melalui API `POST /admin/delay/{n}` dengan body `{"detik":0}`.

### Gateway failover

- Dashboard tetap disajikan launcher di port `8000`; mematikan Node 1 tidak mematikan halaman dashboard.
- Dashboard mencoba gateway API lain (`8002–8004`) bila gateway terpilih tidak tersedia.
- Pergantian gateway hanya mengubah host yang menerima request REST. Ini tidak menghidupkan node target atau mengubah target data.
- Read failover data bergantung pada dukungan backend dan tersedianya snapshot Node 4.
- Jangan matikan Node 4 saat menguji read failover atau LockManager cadangan.
- Stop seluruh proses melalui `POST /launcher/stop_all` atau `Ctrl+C` pada terminal launcher.

## 10. Replika & Info

1. Buka tab kanan **Replika & Info**.
2. **Gateway Aktif** menunjukkan gateway node API terakhir yang berhasil digunakan dan latensi polling.
3. Tabel versi membandingkan versi primer Node 1–3 dengan snapshot Node 4:
   - **SYNC**: versi cocok.
   - **STALE**: versi berbeda; snapshot mungkin tertinggal.
   - **WAIT**: salah satu versi belum tersedia.
4. Klik **Paksa Sinkron Ulang** untuk meminta Node 4 menarik snapshot terbaru.
5. Jika sinkronisasi parsial, pastikan node sumber hidup lalu coba ulang.

Push replication setelah mutasi bersifat asynchronous. Ketidakcocokan versi sesaat setelah transaksi dapat terjadi; tunggu polling atau sinkronkan ulang.

## 11. Seed dan Audit MPI

### Seed Data

Klik **Seed Data** di header untuk menyiapkan katalog pada node aktif dan memicu sinkronisasi Node 4. Gunakan sebelum demo atau saat ingin menyiapkan ulang data awal. Baca toast bila sebagian node gagal.

### Audit MPI

1. Pastikan dokumen data Node 1–4 terisi. Jalankan Seed Data bila perlu.
2. Klik **Audit MPI**.
3. Modal menampilkan valuasi total, jumlah stok kritis, jumlah discrepancy replika, total stok, dan tabel SKU prioritas/turnover.
4. Periksa mode: `MPIEXEC` berarti runtime MPI nyata; `FALLBACK SIMULASI` berarti backend menjalankan simulasi empat rank karena MPI tidak tersedia atau gagal.
5. Event berprotokol `MPI` muncul pada animasi dan log.

Fallback mendukung demonstrasi hasil dan UI, tetapi bukan komputasi paralel MPI nyata. Audit membaca dokumen data; lakukan transaksi dan sinkronisasi lebih dahulu agar hasil merefleksikan kondisi yang diinginkan.

## 12. Memahami notifikasi dan error

- **Sukses**: backend menerima operasi. Untuk push replikasi asynchronous, sukses mutasi belum tentu berarti Node 4 sudah langsung sinkron.
- **Warning**: gateway berpindah, hasil parsial, delay, atau simulasi rollback. Periksa trace.
- **Error**: validasi gagal, node tidak tersedia, stok kurang, SKU duplikat, atau operasi jaringan gagal.
- Jika mutasi timeout, jangan langsung mengulanginya. Mutasi mungkin sempat diterapkan sebelum respons hilang. Periksa stok dan versi terlebih dahulu.
- Error yang memiliki trace tetap dapat menampilkan langkah rollback/failover.

## 13. Urutan demo yang disarankan

1. Jalankan klaster dan tunggu empat node online.
2. Klik **Seed Data**, lalu cek **Replika & Info**.
3. Lakukan Restock SKU pada Node 2 dan amati push replication.
4. Transfer Node 2 ke Node 3 secara normal.
5. Ulangi dengan switch simulasi gagal aktif; amati rollback dan unlock.
6. Cari SKU melalui Chord DHT.
7. Matikan Node 2 di Chaos & Node dan amati heartbeat/read failover yang didukung backend.
8. Hidupkan Node 2, tunggu recovery, lalu sinkron ulang replika.
9. Jalankan Audit MPI dan jelaskan mode aktualnya.
10. Bersihkan log tampilan bila ingin mengulang demo.

## 14. Batasan penting

- Browser berkomunikasi melalui REST; ia tidak menghubungi XML-RPC atau Pyro5 secara langsung.
- Inventori disimpan per node. Penambahan SKU di satu node tidak otomatis membuat semua katalog cabang identik.
- Node 4 adalah replika/failover, bukan sumber primer transaksi kasir.
- Launcher hanya mencatat dan mengendalikan proses node yang dijalankannya. Node yang dijalankan manual mungkin tidak muncul sebagai proses `running` di launcher.
- Akses multi-host memerlukan konfigurasi host, firewall, dan jaringan/CORS yang sesuai; `127.0.0.1` ditujukan untuk demo satu komputer.