import * as React from 'react';
import Container from '@mui/material/Container';
import Box from '@mui/material/Box';
import Copyright from '../src/Copyright';
import TrackTable from '../components/TrackTable';
import Headbar from '../components/Headbar';

export interface ISongData {
  id: string;
  album_type: string;
  song_title: string;
  release_date: string;
  artists: string;
  category: string;
}

interface IResult {
  result: ISongData[];
}

export async function getServerSideProps() {
  const DBHOST = process.env.DBHOST || "localhost";
  const DBUSER = process.env.DBUSER || "postgres";
  const DBPORT = Number(process.env.DBPORT) || 5432;
  const DBDATABASE = process.env.DBDATABASE || "postgres";
  const DBPASSWORD = process.env.DBPASSWORD || "supersicher";
  
  const { Pool } = require('pg');

  const pool = new Pool({
    host: DBHOST,
    user: DBUSER,
    port: DBPORT,
    database: DBDATABASE,
    password: DBPASSWORD
  });

  let result: IResult[] = [];

  try {
    console.log('Try to get data');
    pool.query(
      "SELECT * FROM categorized_tracks ORDER BY song_title;"
      , (err: any, res: any) => {
        if (err) {
          console.log(err.stack);
        } else {
          result = res.rows;
        }
      });
  } finally {
    await pool.end()
  }
  return {
    props: {
      result
    }
  }
}


export default function Index({ result }: IResult) {
  return (
    <Headbar>
      <Container>
        <Box sx={{ my: 4 }}>
          <TrackTable rows={result} />
          <Copyright />
        </Box>
      </Container>
    </Headbar>
  );
}

