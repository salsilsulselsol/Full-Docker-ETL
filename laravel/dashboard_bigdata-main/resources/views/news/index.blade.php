@extends('layouts.app')

@section('content')
<div class="col-md-10 main-content">
    {{-- Search Bar --}}
    <div class="search-container mb-4">
        <div class="search-bar">
            <i class="bi bi-search"></i>
            <input type="text" id="newsSearch" placeholder="Cari berita saham..." autocomplete="off">
        </div>
    </div>

    <div class="content-section">
        <div class="section-title">
            Berita Saham
        </div>

        {{-- News Grid - Hapus row g-4 untuk layout dinamis --}}
        <div class="mt-3" id="newsContainer">
            @include('news.news_items', ['news' => $news])
        </div>

        {{-- Pagination --}}
        <div class="mt-5" id="paginationContainer">
            {{ $news->links('layouts.pagination') }}
        </div>
    </div>
</div>

<script>
$(document).ready(function() {
    let searchTimeout;
    
    $('#newsSearch').on('input', function() {
        clearTimeout(searchTimeout);
        const query = $(this).val().trim();
        
        searchTimeout = setTimeout(function() {
            if (query.length === 0 || query.length >= 3) {
                fetchNews(query);
            }
        }, 500);
    });

    function fetchNews(query = '') {
        $.ajax({
            url: '{{ route("news.search") }}',
            type: 'GET',
            data: { search: query },
            success: function(response) {
                $('#newsContainer').html(response.newsHtml);
                
                if (response.paginationHtml) {
                    $('#paginationContainer').html(response.paginationHtml);
                } else {
                    $('#paginationContainer').empty();
                }
            },
            error: function(xhr) {
                console.error('Search error:', xhr.responseText);
            }
        });
    }
});
</script>
@endsection