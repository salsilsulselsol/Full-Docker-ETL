@extends('layouts.app')

@section('content')
<div class="col-md-10 main-content">
    <div class="search-container">
        <div class="search-bar">
            <i class="bi bi-search"></i>
            <input type="text" placeholder="Cari saham & lainnya">
        </div>
    </div>
    
    <div class="content-section">
        <div class="section-title">Berita Saham</div>

        {{-- 
            PENDEKATAN TERAKHIR:
            1. Wadah luar `.d-flex` akan membuat area berita dan area tombol sama tinggi (default align-items: stretch).
            2. Area tombol (`.ps-4`) kita jadikan flex container juga untuk menengahkan tombol di dalamnya.
        --}}
        <div class="d-flex">

            {{-- 1. Area untuk kartu berita yang bisa di-scroll --}}
            <div class="flex-grow-1" style="overflow: hidden; padding-top: 5px; padding-bottom: 5px;">
                <div class="row g-3 horizontal-scroll-row">
                    @forelse ($news as $item)
                        <div class="news-card-wrapper">
                            <a href="{{ route('news.index') }}" class="text-decoration-none">
                                <div class="news-card h-100">
                                    <div class="news-body">
                                        <div class="news-title">{{ $item['title'] }}</div>
                                        <div class="news-date">{{ \Carbon\Carbon::createFromFormat('l d/M/Y \a\t H:i', $item['original_date'])->locale('id')->translatedFormat('l, d F Y H:i') }}</div>
                                        <div class="news-content">
                                            {{ $item['summary'] }}
                                        </div>
                                    </div>
                                </div>
                            </a>
                        </div>
                    @empty
                        <div class="col">
                            <p>Tidak ada berita yang tersedia saat ini.</p>
                        </div>
                    @endforelse
                </div>
            </div>

            {{-- 2. Area untuk tombol "Lihat Semua" --}}
            {{-- Wadah ini akan meregang tingginya, dan menengahkan tombol di dalamnya --}}
            <div class="ps-4 d-flex align-items-center">
                <a href="{{ route('news.index') }}" class="btn btn-light rounded-circle d-flex align-items-center justify-content-center" style="width: 45px; height: 45px;">
                    <i class="bi bi-arrow-right"></i>
                </a>
            </div>

        </div>
    </div>
    
    <div class="content-section">
        <div class="row g-3">
            <div class="col-md-8">
                <div class="section-title">
                    Grafik Saham
                    <i class="bi bi-three-dots three-dots"></i>
                </div>
                
                <div class="chart-container">
                    <div class="d-flex align-items-center mb-2">
                        <div class="chart-symbol">S&P</div>
                        <div class="chart-name">S&P 500</div>
                    </div>
                    
                    <div class="d-flex justify-content-between align-items-center mb-2">
                        <div class="d-flex align-items-center">
                            <div class="chart-price">4,566.48</div>
                            <div class="chart-change">+1.66%</div>
                        </div>
                        <div class="chart-tabs">
                            <div class="chart-tab" data-period="1d">1d</div>
                            <div class="chart-tab" data-period="5d">5d</div>
                            <div class="chart-tab" data-period="1m">1m</div>
                            <div class="chart-tab" data-period="6m">6m</div>
                            <div class="chart-tab" data-period="1y">1y</div>
                            <div class="chart-tab" data-period="5y">5y</div>
                            <div class="chart-tab active" data-period="max">Max</div>
                        </div>
                    </div>
                    
                    <div class="chart-info">
                        Oct 25, 5:26:38PM UTC-4 · INDEXSP · Disclaimer
                    </div>
                    
                    <div class="chart-canvas-container">
                        <canvas id="stockChart"></canvas>
                        <div id="chartTooltip">
                            <div class="tooltip-date">Oct 25, 5:26:38PM</div>
                            <div class="tooltip-value">4,487.90</div>
                        </div>
                    </div>
                    
                    <div class="chart-timeline">
                        <div>Jul 2023</div>
                        <div>Aug 2023</div>
                        <div>Sep 2023</div>
                        <div>Oct 2023</div>
                        <div>Nov 2023</div>
                    </div>
                </div>
            </div>
            
            <div class="col-md-4">
                <div class="section-title">
                    Rincian
                    <i class="bi bi-three-dots three-dots"></i>
                </div>
                
                <div class="details-container">
                    {{-- BUAT WRAPPER BARU UNTUK MENYEJAJARKAN ITEM --}}
                    <div class="d-flex justify-content-between align-items-center mb-3">
                        
                        {{-- 1. Badge Market Cap (tambahkan class mb-0) --}}
                        <div class="market-cap-badge mb-0">
                            <div class="market-cap-icon">
                                <i class="bi bi-bar-chart-fill"></i>
                            </div>
                            <div>
                                <div class="market-cap-label">Market Cap</div>
                                <div class="market-cap-value">$40.3 T</div>
                            </div>
                        </div>

                        {{-- 2. Dropdown rentang waktu yang baru --}}
                        <div class="dropdown">
                        <button class="btn range-dropdown-btn dropdown-toggle" type="button" data-bs-toggle="dropdown" aria-expanded="false">
                            24 h
                        </button>
                        <ul class="dropdown-menu dropdown-menu-end">
                            <li><a class="dropdown-item" href="#">24 h</a></li>
                            <li><a class="dropdown-item" href="#">7 d</a></li>
                            <li><a class="dropdown-item" href="#">1 m</a></li>
                            <li><a class="dropdown-item" href="#">1 y</a></li>
                        </ul>
                        </div>
                    </div>
                    
                    <table class="details-table">
                         <tr>
                            <td>S&P 500</td>
                            <td>
                                <div class="d-flex align-items-center justify-content-end">
                                    <div>4,566.48</div>
                                </div>
                            </td>
                        </tr>
                        <tr>
                            <td>Previous Close</td>
                            <td>4,566.48</td>
                        </tr>
                        <tr>
                            <td>Day Range</td>
                            <td>4,533.94–4,598.53</td>
                        </tr>
                        <tr>
                            <td>Year Range</td>
                            <td>3,233.94–4,598.53</td>
                        </tr>
                        <tr>
                            <td>Market Cap</td>
                            <td>$40.3 T USD</td>
                        </tr>
                        <tr>
                            <td>Volume</td>
                            <td>2,924,736</td>
                        </tr>
                        <tr>
                            <td>Dividend Yield</td>
                            <td>1.43%</td>
                        </tr>
                        <tr>
                            <td>P/E Ratio</td>
                            <td>31.08</td>
                        </tr>
                        <tr>
                            <td>Previous Close</td>
                            <td>INDEX</td>
                        </tr>
                        <tr>
                            <td>Atribut lainnya</td>
                            <td></td>
                        </tr>
                    </table>
                </div>
            </div>
        </div>
    </div>
</div>
@endsection