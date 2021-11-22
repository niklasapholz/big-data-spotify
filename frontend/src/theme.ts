import { createTheme } from '@mui/material/styles';
import { red, grey } from '@mui/material/colors';

// Create a theme instance.
const theme = createTheme({
  palette: {
    primary: {
      main: '#000',
    },
    secondary: {
      main: '#19857b',
    },
    error: {
      main: red.A400,
    },
    background: {
      default: '#121212',
      paper: '#121212',
    },
    text: {
      primary: '#fff',
      secondary: grey[500],
    }
  },
});

export default theme;