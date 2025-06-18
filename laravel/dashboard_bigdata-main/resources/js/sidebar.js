// File: resources/js/sidebar.js

document.addEventListener('DOMContentLoaded', function () {
    // Cari semua item menu yang memiliki submenu
    const menuItemsWithSubmenu = document.querySelectorAll('.has-submenu');

    menuItemsWithSubmenu.forEach(item => {
        item.addEventListener('click', function (event) {
            // Mencegah link default a href="#" bekerja
            event.preventDefault();

            // Cari elemen submenu yang berada tepat setelah item menu ini
            const submenu = this.nextElementSibling;

            // Toggle class 'active' pada item menu (untuk rotasi ikon)
            this.classList.toggle('active');

            // Toggle class 'show' pada submenu untuk menampilkannya
            if (submenu && submenu.classList.contains('submenu')) {
                submenu.classList.toggle('show');
            }
        });
    });
});