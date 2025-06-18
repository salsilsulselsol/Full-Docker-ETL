<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;

class DetailSahamController extends Controller
{
    public function index()
    {
        // Mengarahkan ke file 'detail.blade.php' di dalam folder 'resources/views/idx/'
        return view('idx.detail');
    }
}