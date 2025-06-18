@forelse ($news as $item)
    {{-- Hapus class grid kolom agar layout dinamis --}}
    <div class="news-item-dynamic">
        <div class="news-card-wrapper">
            <div class="news-card">
                <div class="news-body">
                    <div class="news-title">{{ $item['title'] }}</div>
                    <div class="news-date">
                        {{ \Carbon\Carbon::createFromFormat('l d/M/Y \a\t H:i', $item['original_date'])->locale('id')->translatedFormat('l, d F Y H:i') }}
                    </div>
                    <div class="news-content">
                        {{-- Tampilkan ringkasan berita --}}
                        {{ $item['summary'] }}
                    </div>
                </div>
            </div>
        </div>
    </div>
@empty
    <div class="news-item-dynamic">
        <div class="alert alert-info">
            Tidak ada berita yang ditemukan.
        </div>
    </div>
@endforelse
