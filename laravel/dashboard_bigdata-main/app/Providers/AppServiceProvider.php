<?php

namespace App\Providers;
use Illuminate\Pagination\Paginator;
use Carbon\Carbon;

use Illuminate\Support\ServiceProvider;

class AppServiceProvider extends ServiceProvider
{
    /**
     * Register any application services.
     */
    public function register(): void
    {
        //
    }

    /**
     * Bootstrap any application services.
     */
    public function boot(): void
    {
        //
          Paginator::useBootstrapFive();
          Carbon::setLocale('id');
    }
}
