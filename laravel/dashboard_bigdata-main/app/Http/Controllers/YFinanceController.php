<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Illuminate\Pagination\LengthAwarePaginator;
use Illuminate\Support\Collection;
use PhpOffice\PhpSpreadsheet\IOFactory;

class YFinanceController extends Controller
{
    /**
     * Membaca dan mem-parsing data emiten dari file CSV.
     *
     * @return Collection
     */
    private function getEmitenData()
    {
        $path = public_path('Daftar_Saham.xlsx');
        if (!file_exists($path)) {
            return collect(); // Kembalikan koleksi kosong jika file tidak ada
        }

        // Baca file XLSX menggunakan PhpSpreadsheet
        $spreadsheet = IOFactory::load($path);
        $worksheet = $spreadsheet->getActiveSheet();

        $data = [];
        foreach ($worksheet->getRowIterator() as $rowIndex => $row) {
            // Lewati baris header (baris pertama)
            if ($rowIndex === 1) continue;

            $cellIterator = $row->getCellIterator();
            $cellIterator->setIterateOnlyExistingCells(false);

            $rowData = [];
            foreach ($cellIterator as $cell) {
                $rowData[] = trim($cell->getValue());
            }

            // Pastikan kolom Kode dan Nama Perusahaan ada
            if (count($rowData) >= 3 && !empty($rowData[1]) && !empty($rowData[2])) {
                $data[] = [
                    'Kode' => $rowData[1],
                    'Nama Perusahaan' => $rowData[2],
                    'Tanggal Pencatatan' => $rowData[3] ?? null,
                    'Saham' => $rowData[4] ?? null,
                    'Papan Pencatatan' => $rowData[5] ?? null,
                ];
            }
        }

        return new Collection($data);
    }

    /**
     * Menampilkan halaman daftar emiten dengan paginasi dan pencarian.
     *
     * @param Request $request
     * @return \Illuminate\View\View|\Illuminate\Http\JsonResponse
     */
    public function index(Request $request)
    {
        $allEmiten = $this->getEmitenData();
        $searchQuery = $request->input('search', '');

        // Filter data jika ada query pencarian
        if (!empty($searchQuery)) {
            $allEmiten = $allEmiten->filter(function ($item) use ($searchQuery) {
                // Cari berdasarkan Kode atau Nama Perusahaan (case-insensitive)
                return stripos($item['Kode'], $searchQuery) !== false ||
                       stripos($item['Nama Perusahaan'], $searchQuery) !== false;
            });
        }

        // Paginasi manual untuk koleksi data
        $perPage = 25; // Jumlah item per halaman
        $currentPage = LengthAwarePaginator::resolveCurrentPage();
        $currentPageItems = $allEmiten->slice(($currentPage - 1) * $perPage, $perPage)->values();
        
        $paginatedItems = new LengthAwarePaginator(
            $currentPageItems,
            count($allEmiten),
            $perPage,
            $currentPage,
            ['path' => $request->url(), 'query' => $request->query()]
        );

        // Jika ini adalah request AJAX, kirim kembali hanya partial HTML
        if ($request->ajax()) {
            $tableHtml = view('yfinance.table_data', ['emiten' => $paginatedItems])->render();
            $paginationHtml = $paginatedItems->links('layouts.pagination')->toHtml();

            return response()->json(['tableHtml' => $tableHtml, 'paginationHtml' => $paginationHtml]);
        }
        
        // Jika request biasa, tampilkan view lengkap
        return view('yfinance.index', ['emiten' => $paginatedItems]);
    }
}