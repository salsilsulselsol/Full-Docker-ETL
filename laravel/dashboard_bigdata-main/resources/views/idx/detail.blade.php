@extends('layouts.app')

@push('styles')
<style>
    /* Menggunakan style dari app.css & dashboard untuk konsistensi */
    .main-content {
        /* Padding diatur oleh .search-container dan .content-section */
    }
    .report-card {
        background-color: white;
        border: 1px solid #eef రాష్ట్ర;
        border-radius: 12px;
        padding: 1.25rem;
        transition: all 0.2s ease-in-out;
        height: 100%;
        display: flex;
        flex-direction: column;
    }
    .report-card:hover {
        transform: translateY(-5px);
        box-shadow: 0 8px 25px rgba(0,0,0,0.07);
    }
    .card-header-flex {
        display: flex;
        justify-content: space-between;
        align-items: center;
        margin-bottom: 1rem;
        padding-bottom: 0.75rem;
        border-bottom: 1px solid #eef రాష్ట్ర;
    }
    .card-code { font-weight: 600; font-size: 1.1rem; }
    .card-year-badge { font-size: 0.75rem; font-weight: 500; padding: 0.25rem 0.6rem; border-radius: 20px; background-color: #f0f2f5; }
    .card-data-row { display: flex; justify-content: space-between; font-size: 0.875rem; padding: 0.5rem 0; border-bottom: 1px solid #f7f7f7; }
    .card-data-row:last-child { border-bottom: none; }
    .card-data-label { color: #6c757d; }
    .card-data-value { font-weight: 500; color: #343a40; }
</style>
@endpush

@section('content')
<div class="col-md-10 main-content">

    {{-- 1. SEARCH BAR DI ATAS (Meniru struktur dashboard.blade.php) --}}
    <div class="search-container">
        <div class="search-bar">
            <i class="bi bi-search"></i>
            <input type="text" id="search-input" placeholder="Cari saham & lainnya...">
        </div>
    </div>
    
    {{-- 2. KONTEN UTAMA DIBUNGKUS DALAM .content-section --}}
    <div class="content-section">
        
        {{-- Header: Judul & Filter --}}
        <div class="d-flex justify-content-between align-items-center mb-4">
            <div class="section-title mb-0">Laporan Keuangan</div>
            <div class="d-flex align-items-center">
                <select id="year-filter" class="form-select me-2" style="width: 120px; border-radius: 20px;">
                    <option value="2025">2025</option>
                    <option value="2024">2024</option>
                    <option value="2023" selected>2023</option>
                    <option value="2022">2022</option>
                    <option value="2021">2021</option>
                </select>
                <select id="quarter-filter" class="form-select" style="width: 150px; border-radius: 20px;">
                    <option value="audit">Tahunan</option>
                    <option value="tw3">Triwulan 3</option>
                    <option value="tw2">Triwulan 2</option>
                    <option value="tw1">Triwulan 1</option>
                </select>
            </div>
        </div>

        {{-- Grid untuk Kartu --}}
        <div id="reports-grid" class="row">
            {{-- Kartu akan dimuat di sini oleh JavaScript --}}
        </div>

        {{-- Pagination --}}
        <div id="pagination-container" class="d-flex justify-content-center mt-4">
            {{-- Pagination akan dimuat di sini --}}
        </div>

    </div>
</div>
@endsection

@push('scripts')
{{-- Kode JavaScript ini memanggil API Flask --}}
<script>
$(document).ready(function() {
    const searchInput = $('#search-input'); 
    const API_BASE_URL = 'http://127.0.0.1:5000/api/reports/list'; // URL ke API Flask Anda
    let searchTimeout;

    function fetchReports(page = 1) {
        const year = $('#year-filter').val();
        const quarter = $('#quarter-filter').val();
        const searchQuery = searchInput.val();
        const API_URL = `${API_BASE_URL}/${year}/${quarter}`;

        $('#reports-grid').html('<div class="col-12 text-center p-5"><span class="spinner-border spinner-border-sm"></span> Memuat data...</div>');
        $('#pagination-container').empty();

        $.ajax({
            url: API_URL,
            type: 'GET',
            data: { page: page, search: searchQuery, limit: 9 },
            success: function(response) {
                renderCards(response.data);
                renderPagination(response.page, response.total_count, response.limit);
            },
            error: function(xhr) {
                 const errorMessage = xhr.responseJSON ? xhr.responseJSON.message : 'Gagal memuat data. Periksa koneksi ke API dan error di Console.';
                 $('#reports-grid').html(`<div class="col-12 text-center p-5 text-danger">${errorMessage}</div>`);
            }
        });
    }

    function renderCards(reports) {
        // ... (Fungsi renderCards tetap sama seperti sebelumnya, tidak perlu diubah) ...
        const grid = $('#reports-grid');
        grid.empty();
        if (!reports || reports.length === 0) {
            grid.html('<div class="col-12 text-center p-5">Tidak ada data ditemukan.</div>');
            return;
        }
        reports.forEach(report => {
            const displaySector = (report.subsector || 'N/A').replace(/^\d+\.\s*/, '');
            const formatCompact = (val) => val != null ? new Intl.NumberFormat('id-ID', { notation: 'compact', compactDisplay: 'short' }).format(val) : 'N/A';
            const cardHtml = `
                <div class="col-lg-4 col-md-6 mb-4">
                    <div class="report-card">
                        <div class="card-header-flex">
                            <div><div class="card-code">${report.company_code}</div><small class="text-muted">${report.company_name || ''}</small></div>
                            <span class="card-year-badge">${report.year}</span>
                        </div>
                        <div class="flex-grow-1">
                            <div class="card-data-row"><span class="card-data-label">Sub Sektor</span><span class="card-data-value">${displaySector}</span></div>
                            <div class="card-data-row"><span class="card-data-label">Pendapatan</span><span class="card-data-value">${formatCompact(report.revenue)}</span></div>
                            <div class="card-data-row"><span class="card-data-label">Laba Bersih</span><span class="card-data-value">${formatCompact(report.net_profit_loss)}</span></div>
                            <div class="card-data-row"><span class="card-data-label">Total Aset</span><span class="card-data-value">${formatCompact(report.total_assets)}</span></div>
                            <div class="card-data-row"><span class="card-data-label">Total Ekuitas</span><span class="card-data-value">${formatCompact(report.total_equity)}</span></div>
                            <div class="card-data-row"><span class="card-data-label">DER</span><span class="card-data-value">${report.debt_to_equity_ratio ? report.debt_to_equity_ratio.toFixed(2) : 'N/A'}</span></div>
                        </div>
                    </div>
                </div>`;
            grid.append(cardHtml);
        });
    }

    function renderPagination(currentPage, totalItems, limit) {
        // ... (Fungsi renderPagination tetap sama seperti sebelumnya, tidak perlu diubah) ...
        const totalPages = Math.ceil(totalItems / limit);
        const container = $('#pagination-container');
        container.empty();
        if (totalPages <= 1) return;
        const pagination = $('<ul class="pagination"></ul>');
        pagination.append(`<li class="page-item ${currentPage === 1 ? 'disabled' : ''}"><a class="page-link" href="#" data-page="${currentPage - 1}">«</a></li>`);
        for (let i = 1; i <= totalPages; i++) {
             pagination.append(`<li class="page-item ${i === currentPage ? 'active' : ''}"><a class="page-link" href="#" data-page="${i}">${i}</a></li>`);
        }
        pagination.append(`<li class="page-item ${currentPage === totalPages ? 'disabled' : ''}"><a class="page-link" href="#" data-page="${currentPage + 1}">»</a></li>`);
        container.append(pagination);
    }

    // ... (Semua event listener tetap sama, tidak perlu diubah) ...
    $('#year-filter, #quarter-filter').on('change', () => fetchReports(1));
    $('#search-input').on('keyup', () => {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(() => fetchReports(1), 500);
    });
    $(document).on('click', '#pagination-container a', function(e) {
        e.preventDefault();
        const page = $(this).data('page');
        if (page) fetchReports(page);
    });

    fetchReports();
});
</script>
@endpush