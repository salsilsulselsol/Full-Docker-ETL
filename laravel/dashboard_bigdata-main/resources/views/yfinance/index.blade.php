@extends('layouts.app')

@section('content')
<div class="col-md-10 main-content">
    {{-- Search Bar --}}
    <div class="search-container mb-4">
        <div class="search-bar">
            <i class="bi bi-search"></i>
            <input type="text" id="emitenSearch" placeholder="Cari saham berdasarkan kode atau nama..." autocomplete="off">
        </div>
    </div>

    <div class="content-section">
        <div class="section-title">
            List Emiten
        </div>

        {{-- Container untuk tabel agar mudah di-update via AJAX --}}
        <div class="table-responsive mt-3" id="emitenTableContainer">
            {{-- Memuat partial view untuk tabel saat halaman pertama kali dibuka --}}
            @include('yfinance.table_data', ['emiten' => $emiten])
        </div>

        {{-- Container untuk Pagination --}}
        <div class="mt-4 d-flex justify-content-center" id="paginationContainer">
            {{ $emiten->links('layouts.pagination') }}
        </div>
    </div>
</div>

<script>
$(document).ready(function() {
    let searchTimeout;

    // Fungsi untuk mengambil data dengan AJAX
    function fetchEmiten(query = '', page = 1) {
        // Tampilkan indikator loading jika perlu
        // $('#emitenTableContainer').html('<tr><td colspan="6" class="text-center">Loading...</td></tr>');

        $.ajax({
            url: `{{ route('emiten.index') }}?page=${page}`,
            type: 'GET',
            data: { 
                search: query 
            },
            success: function(response) {
                $('#emitenTableContainer').html(response.tableHtml);
                $('#paginationContainer').html(response.paginationHtml);
            },
            error: function(xhr) {
                console.error('Terjadi kesalahan:', xhr.responseText);
                $('#emitenTableContainer').html('<tr><td colspan="6" class="text-center text-danger">Gagal memuat data.</td></tr>');
            }
        });
    }

    // Event handler untuk input pencarian dengan debounce
    $('#emitenSearch').on('input', function() {
        clearTimeout(searchTimeout);
        const query = $(this).val().trim();
        
        searchTimeout = setTimeout(function() {
             fetchEmiten(query, 1); // Selalu kembali ke halaman 1 saat pencarian baru
        }, 500); // Tunggu 500ms setelah user berhenti mengetik
    });

    // Event handler untuk klik pagination (menggunakan event delegation)
    $(document).on('click', '#paginationContainer .pagination a', function(event) {
        event.preventDefault(); 
        const pageUrl = $(this).attr('href');
        const page = new URL(pageUrl).searchParams.get('page');
        const query = $('#emitenSearch').val().trim();
        
        fetchEmiten(query, page);
    });
});
</script>
@endsection