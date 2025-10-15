/** @type {import('tailwindcss').Config} */
module.exports = {
  content: [
    "./src/**/*.{js,jsx,ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        'navy': {
          'light': '#1e293b', // slate-800
          'DEFAULT': '#0f172a', // slate-900
          'dark': '#020617',   // slate-950
        },
        'brand': {
          'DEFAULT': '#6366f1', // indigo-500
          'light': '#818cf8',   // indigo-400
        }
      }
    },
  },
  plugins: [],
}
