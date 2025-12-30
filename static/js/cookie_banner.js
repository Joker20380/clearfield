document.addEventListener('DOMContentLoaded', () => {
  const banner = document.getElementById('cookie-banner');
  const btn = document.getElementById('accept-cookies');
  if (!banner || !btn) return;

  // показать с небольшой задержкой
  setTimeout(() => {
    banner.classList.add('show');
  }, 300);

  btn.addEventListener('click', () => {
    banner.classList.remove('show');
    document.cookie = "cookies_accepted=true; path=/; max-age=" + 60*60*24*365;

    setTimeout(() => {
      banner.remove();
    }, 600);
  });
});
