import * as React from 'react';
import { DataGrid, GridColDef } from '@mui/x-data-grid';
import { ISongData } from '../pages';

interface ITrackTable{
  rows: ISongData[];
}

const columns: GridColDef[] = [
  {
    field: 'song_title',
    headerName: 'Title',
    editable: true,
    minWidth: 160,
    flex: 1,
  },
  {
    field: 'artists',
    headerName: 'Artists',
    editable: true,
    minWidth: 160,
    flex: 1,
  },
  {
    field: 'release_date',
    headerName: 'Release date',
    width: 160,
  },
  {
    field: 'category',
    headerName: 'Category',
    width: 100,
  },
];



export default function TrackTable({rows}: ITrackTable) {
  return (
    <div style={{ height: '80vh', width: '100%', padding: '16px' }}>
      <DataGrid
        rows={rows}
        columns={columns}
        pageSize={25}
        rowsPerPageOptions={[5]}
        disableSelectionOnClick
      />
    </div>
  );
}