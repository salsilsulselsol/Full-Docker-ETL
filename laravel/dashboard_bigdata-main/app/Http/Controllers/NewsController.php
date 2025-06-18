<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Illuminate\Support\Facades\Http; 
use Illuminate\Pagination\LengthAwarePaginator;
use Illuminate\Support\Collection;

class NewsController extends Controller
{
    /**
     * Menampilkan halaman dashboard dengan data berita dari API.
     */
    public function index()
    {
        $newsData = []; // Siapkan array kosong sebagai nilai default

        try {
            // Ambil data dari API lokal
            $response = Http::get('http://localhost:5000/api/iqplus/news');

            if ($response->successful()) {
                $allData = $response->json()['data'] ?? [];
                // Ambil maksimal 10 berita pertama
                $newsData = array_slice($allData, 0, 10);
            }
        } catch (\Exception $e) {
            // Jika gagal, bisa tambahkan logging di sini jika perlu
            // Log::error("Gagal mengambil berita dari API: " . $e->getMessage());
        }

        return view('dashboard', ['news' => $newsData]);
    }

    /**
     * Menampilkan semua berita dengan fitur pencarian dan paginasi.
     */
    public function showAllNews(Request $request)
    {
        $allNewsData = [];

        try {
            $response = Http::get('http://localhost:5000/api/iqplus/news');
            if ($response->successful()) {
                $allNewsData = $response->json()['data'] ?? [];
            }
        } catch (\Exception $e) {
            // Jika error API, data akan tetap kosong
        }

        $perPage = 30; // Jumlah berita per halaman
        $currentPage = $request->input('page', 1);
        $collection = new Collection($allNewsData);

        // Filter pencarian jika ada kata kunci
        if ($request->has('search') && !empty($request->search)) {
            $searchTerm = strtolower($request->search);
            $collection = $collection->filter(function ($item) use ($searchTerm) {
                return str_contains(strtolower($item['title']), $searchTerm);
            });
        }

        // Ambil item sesuai halaman saat ini
        $currentPageItems = $collection->slice(($currentPage - 1) * $perPage, $perPage)->all();
        
        // Buat objek paginator manual
        $paginatedItems = new LengthAwarePaginator($currentPageItems, count($collection), $perPage, $currentPage, [
            'path' => $request->url(),
            'query' => $request->query(),
        ]);

        // Jika permintaan AJAX, kembalikan dalam format JSON
        if ($request->ajax() || $request->wantsJson()) {
            return response()->json([
                'newsHtml' => view('news.news_items', ['news' => $paginatedItems])->render(),
                'paginationHtml' => $paginatedItems->links('layouts.pagination')->toHtml()
            ]);
        }

        return view('news.index', ['news' => $paginatedItems]);
    }
}
