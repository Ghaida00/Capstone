# Diseminasi Peakload — Panduan Publish WordPress

Bundle siap publish untuk [filkom.ub.ac.id/project](https://filkom.ub.ac.id/project/).

## Isi folder

| File | Fungsi |
|------|--------|
| [`content.md`](content.md) | Teks lengkap 14 section (copy-paste) |
| [`wordpress-blocks.html`](wordpress-blocks.html) | HTML blocks untuk Kadence |
| [`custom.css`](custom.css) | Styles dark — paste ke Additional CSS |
| [`custom-light.css`](custom-light.css) | **Light variant** (FILKOM) — paste setelah `custom.css` |
| [`preview-light.html`](preview-light.html) | Preview lokal tema terang |
| [`team.json`](team.json) | Metadata tim + metrik (machine-readable) |
| [`demo-script.md`](demo-script.md) | Script demo 18 langkah + video ringkas |
| [`screenshots-manifest.md`](screenshots-manifest.md) | Daftar aset gambar |
| [`assets/curated/`](assets/curated/) | 12 screenshot siap upload |
| [`assets/hero-peakload.svg`](assets/hero-peakload.svg) | Featured image / OG |
| [`preview.html`](preview.html) | Preview lokal (butuh HTTP server) |

## Slug URL (disarankan)

```
optimasi-skalabilitas-transaksi-perbankan-peakload-management-read-write-separation-rust
```

## Langkah publish di WordPress

### 1. CSS global

**Tema terang (disarankan untuk filkom.ub.ac.id):**

1. Login WordPress filkom.ub.ac.id
2. **Appearance → Customize → Additional CSS**
3. Paste seluruh isi [`custom.css`](custom.css), lalu paste [`custom-light.css`](custom-light.css) di bawahnya
4. Tambahkan class `pl-theme-light` pada wrapper HTML block terluar: `<div class="pl-page pl-theme-light">`
5. Publish

**Tema gelap (opsional):**

1. Paste hanya [`custom.css`](custom.css)
2. Wrapper: `<div class="pl-page">` (tanpa `pl-theme-light`)

Preview lokal:

| URL | Tema |
|-----|------|
| http://localhost:8765/docs/dissemination/preview-light.html | Terang |
| http://localhost:8765/docs/dissemination/preview.html?theme=light | Terang |
| http://localhost:8765/docs/dissemination/preview.html | Gelap |

### 2. Upload media

Upload semua file di `assets/curated/` ke **Media Library**.

Upload `assets/hero-peakload.svg` sebagai **Featured Image** (convert ke PNG/WebP di editor jika perlu).

### 3. Buat posting

1. **Posts → Add New**
2. **Title:** Optimasi Skalabilitas Transaksi Perbankan untuk Mengatasi Exploding User Data melalui Peak Load Management dan Read/Write Separation Berbasis Rust
3. **Tags:** `capstone`
4. **Featured image:** hero Peakload

### 4. Paste konten

**Opsi A — Satu block HTML (cepat):**

- Tambah block **Custom HTML** atau **Kadence Advanced HTML**
- Paste isi [`wordpress-blocks.html`](wordpress-blocks.html)
- Ganti semua `src="assets/curated/..."` dengan URL Media Library WordPress

**Opsi B — Section per section:**

- Paste per `<section>...</section>` ke block terpisah
- Lebih mudah edit di editor visual

### 5. Video demo

1. Rekam sesuai [`demo-script.md`](demo-script.md)
2. Upload ke YouTube (unlisted/public)
3. Ganti placeholder di section `#demo`:

```html
<div class="pl-video">
  <iframe src="https://www.youtube.com/embed/VIDEO_ID" title="Peakload Demo" allowfullscreen></iframe>
</div>
```

### 6. Preview & publish

- [ ] Preview mobile — kartu stack, tabel scroll
- [ ] Semua link GitHub benar
- [ ] Tidak ada IP/credential ops internal
- [ ] Metrik: 1M txn/jam, P95 4,43 ms, 0,00% error
- [ ] NIM & email tim benar
- [ ] Link pembimbing: [Lutfi Fanani](https://filkom.ub.ac.id/profile/dosen/lutfi.fanani)

## Preview lokal

Jalankan server dari **root repo** (bukan hanya folder dissemination) agar link Swagger & arsitektur interaktif berfungsi:

```powershell
cd "c:\Users\Ikhsa\Downloads\MINE\Semester 6\Gh\Capstone"
python -m http.server 8765
```

| URL | Apa yang Anda lihat |
|-----|---------------------|
| http://localhost:8765/docs/dissemination/preview.html | Halaman diseminasi lengkap |
| http://localhost:8765/docs/dissemination/swagger-preview.html | **Swagger UI** (bukan YAML mentah) |
| http://localhost:8765/docs/architecture/architecture.html | **Diagram arsitektur interaktif** (bukan HTML source) |

**Catatan:** Link GitHub `blob/.../apiContract.yaml` dan `architecture.html` hanya menampilkan **kode sumber**. Itu normal — GitHub tidak merender Swagger atau app HTML interaktif. Kartu di halaman diseminasi sekarang mengarah ke preview lokal di atas.

### Setelah publish ke WordPress

Ganti link kartu menjadi URL publik:

- **API Contract:** `https://editor.swagger.io/?url=https://raw.githubusercontent.com/Ghaida00/Capstone/main/docs/apiContract.yaml`
- **Arsitektur:** aktifkan GitHub Pages di repo, atau embed screenshot + instruksi clone repo

## Regenerate screenshot dari PDF LK4

```powershell
python docs/dissemination/scripts/curate_lk4_images.py
```

PDF sumber: `LAPORAN LEMBAR KERJA 4_Topik B.4 Kelompok 8 (2).pdf`

## QA checklist

- [ ] Judul & NIM match LK4
- [ ] Bank X = studi kasus (no data confidential)
- [ ] GitHub: https://github.com/Ghaida00/Capstone
- [ ] Video embed 16:9 responsive
- [ ] Gambar < ~500 KB each (compress jika perlu)
- [ ] README repo: clone URL sudah benar

## Kontak tim

| Anggota | Email |
|---------|-------|
| Ghaida Nayla | ghaidanayla@student.ub.ac.id |
| Lovely Ito Panjaitan | lovelyito@student.ub.ac.id |
| Verda Aulia Setri | verdaaulia@student.ub.ac.id |
| Pembimbing | lutfifanani@ub.ac.id |
