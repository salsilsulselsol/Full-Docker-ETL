<table class="table table-hover">
    <thead>
        <tr>
            <th scope="col" style="width: 5%;">No</th>
            <th scope="col" style="width: 10%;">Kode</th>
            <th scope="col">Nama Perusahaan</th>
            <th scope="col">Tanggal Pencatatan</th>
            <th scope="col">Saham</th>
            <th scope="col">Papan Pencatatan</th>
        </tr>
    </thead>
    <tbody>
        @forelse ($emiten as $item)
            <tr>
                <td>{{ ($emiten->currentPage() - 1) * $emiten->perPage() + $loop->iteration }}</td>
                <td><strong>{{ $item['Kode'] }}</strong></td>
                <td>{{ $item['Nama Perusahaan'] }}</td>
                <td>{{ $item['Tanggal Pencatatan'] ?? '-' }}</td>
                <td>{{ $item['Saham'] ?? '-' }}</td>
                <td>{{ $item['Papan Pencatatan'] ?? '-' }}</td>
            </tr>
        @empty
            <tr>
                <td colspan="6" class="text-center py-4">
                    Tidak ada data emiten yang ditemukan.
                </td>
            </tr>
        @endforelse
    </tbody>
</table>