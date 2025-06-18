// File: resources/js/dashboard-chart.js

/**
 * Fungsi ini akan dieksekusi setelah seluruh konten halaman dimuat.
 */
document.addEventListener('DOMContentLoaded', function() {
    // Penting: Cek apakah elemen grafik (#stockChart) ada di halaman saat ini.
    // Ini untuk mencegah error jika script ini dimuat di halaman lain yang tidak memiliki grafik.
    if (!document.getElementById('stockChart')) {
        return;
    }

    // Data dummy untuk grafik
    const chartData = {
        '1d': generateDailyData(),
        '5d': generateDailyData(5),
        '1m': generateMonthlyData(1),
        '6m': generateMonthlyData(6),
        '1y': generateYearlyData(1),
        '5y': generateYearlyData(5),
        'max': generateMaxData()
    };
    
    // Inisialisasi grafik dengan data default (max)
    initChart('max');
    
    // Event listener untuk tab periode
    document.querySelectorAll('.chart-tab').forEach(tab => {
        tab.addEventListener('click', function() {
            document.querySelectorAll('.chart-tab').forEach(t => t.classList.remove('active'));
            this.classList.add('active');
            initChart(this.getAttribute('data-period'));
        });
    });
    
    /**
     * Menginisialisasi atau meng-update grafik Chart.js.
     * @param {string} period Periode waktu (e.g., '1d', '1m', 'max').
     */
    function initChart(period) {
        const ctx = document.getElementById('stockChart').getContext('2d');
        
        // Hapus instance grafik lama jika ada untuk mencegah memory leak
        if (window.stockChart instanceof Chart) {
            window.stockChart.destroy();
        }
        
        const data = chartData[period];
        
        const gradient = ctx.createLinearGradient(0, 0, 0, 200);
        gradient.addColorStop(0, 'rgba(0, 193, 118, 0.4)');
        gradient.addColorStop(1, 'rgba(0, 193, 118, 0)');
        
        // Buat instance grafik baru dan simpan di object window
        window.stockChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: data.labels,
                datasets: [{
                    label: 'S&P 500',
                    data: data.values,
                    borderColor: '#00c176',
                    backgroundColor: gradient,
                    borderWidth: 2,
                    pointRadius: 0,
                    pointHoverRadius: 5,
                    pointHoverBackgroundColor: '#00c176',
                    pointHoverBorderColor: '#fff',
                    pointHoverBorderWidth: 2,
                    tension: 0.4,
                    fill: true
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false
                },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        enabled: false,
                        external: externalTooltipHandler
                    }
                },
                scales: {
                    x: { display: false },
                    y: { display: false }
                }
            }
        });
    }
    
    /**
     * Handler untuk menampilkan tooltip custom saat hover di atas grafik.
     * @param {object} context Konteks dari Chart.js.
     */
    function externalTooltipHandler(context) {
        const tooltipEl = document.getElementById('chartTooltip');
        const tooltipModel = context.tooltip;
        const chart = context.chart;

        // Sembunyikan tooltip jika tidak ada interaksi
        if (tooltipModel.opacity === 0) {
            tooltipEl.style.opacity = 0;
            return;
        }

        // Atur konten teks di dalam tooltip
        if (tooltipModel.body) {
            const titleLines = tooltipModel.title || [];
            const bodyLines = tooltipModel.body.map(b => b.lines);
            
            tooltipEl.querySelector('.tooltip-date').textContent = titleLines[0];
            tooltipEl.querySelector('.tooltip-value').textContent = bodyLines[0];
        }

        // === LOGIKA POSISI YANG DISEMPURNAKAN ===
        const chartWidth = chart.width;
        const tooltipWidth = tooltipEl.offsetWidth;
        
        // Default posisi tooltip adalah di kanan kursor
        let transformX = '15%'; 

        // Jika posisi kursor + lebar tooltip melebihi lebar grafik,
        // pindahkan tooltip ke kiri kursor agar tidak terpotong.
        if (tooltipModel.caretX + tooltipWidth > chartWidth) {
            transformX = '-115%'; // Geser ke kiri
        }
        // === AKHIR LOGIKA BARU ===

        // Tampilkan dan posisikan tooltip
        tooltipEl.style.opacity = 1;
        tooltipEl.style.left = tooltipModel.caretX + 'px';
        tooltipEl.style.top = tooltipModel.caretY + 'px';
        
        // Gunakan transform yang sudah dinamis dan posisikan sedikit lebih jauh dari titik
        tooltipEl.style.transform = `translate(${transformX}, -125%)`;
    }
    
    // Kumpulan fungsi untuk menghasilkan data dummy
    function generateDailyData(days = 1) {
        const labels = [], values = [], baseValue = 4500, now = new Date();
        for (let i = 0; i < 24 * days; i++) {
            const time = new Date(now);
            time.setHours(now.getHours() - (24 * days - i));
            labels.push(time.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }));
            const randomChange = (Math.random() - 0.4) * 20;
            values.push((i === 0 ? baseValue : values[i-1]) + randomChange);
        }
        return { labels, values };
    }

    function generateMonthlyData(months = 1) {
        const labels = [], values = [], baseValue = 4300, now = new Date(), daysInPeriod = 30 * months;
        for (let i = 0; i < daysInPeriod; i++) {
            const date = new Date(now);
            date.setDate(now.getDate() - (daysInPeriod - i));
            labels.push(date.toLocaleDateString([], { month: 'short', day: 'numeric' }));
            const trend = (i / daysInPeriod) * 300, randomChange = (Math.random() - 0.4) * 30;
            values.push((i === 0 ? baseValue : values[i-1]) + randomChange + (trend - (i>0 ? (i-1)/daysInPeriod * 300 : 0)));
        }
        return { labels, values };
    }

    function generateYearlyData(years = 1) {
        const labels = [], values = [], baseValue = 3800, now = new Date(), daysInPeriod = 365 * years, dataPoints = 100, interval = Math.floor(daysInPeriod / dataPoints);
        for (let i = 0; i < dataPoints; i++) {
            const date = new Date(now);
            date.setDate(now.getDate() - (daysInPeriod - i * interval));
            labels.push(date.toLocaleDateString([], { month: 'short', year: 'numeric' }));
            const trend = (i / dataPoints) * 800, randomChange = (Math.random() - 0.4) * 50;
            values.push((i === 0 ? baseValue : values[i-1]) + randomChange + (trend - (i>0 ? (i-1)/dataPoints * 800 : 0)));
        }
        return { labels, values };
    }

    function generateMaxData() {
        const labels = [], values = [], baseValue = 3200, dataPoints = 100;
        const startDate = new Date('2023-07-01'), endDate = new Date('2023-11-30');
        const daysDiff = Math.floor((endDate - startDate) / (1000*60*60*24)), interval = Math.floor(daysDiff / dataPoints);
        for (let i = 0; i < dataPoints; i++) {
            const date = new Date(startDate);
            date.setDate(startDate.getDate() + i * interval);
            labels.push(date.toLocaleDateString([], { month: 'short', day: 'numeric' }));
            let trend;
            if (i < dataPoints * 0.2) trend = (i / (dataPoints * 0.2)) * 300;
            else if (i < dataPoints * 0.4) trend = 300 - ((i - dataPoints * 0.2) / (dataPoints * 0.2)) * 200;
            else if (i < dataPoints * 0.6) trend = 100 + ((i - dataPoints * 0.4) / (dataPoints * 0.2)) * 400;
            else if (i < dataPoints * 0.8) trend = 500 - ((i - dataPoints * 0.6) / (dataPoints * 0.2)) * 200;
            else trend = 300 + ((i - dataPoints * 0.8) / (dataPoints * 0.2)) * 200;
            const randomChange = (Math.random() - 0.5) * 30;
            values.push(baseValue + trend + randomChange);
        }
        return { labels, values };
    }
});