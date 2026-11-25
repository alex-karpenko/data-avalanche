// The oauth2 sign-in page goes to the provider by itself after a few seconds, unless the person
// stays (Admin_Console §9). Without this script, the button is all there is.
(function () {
  var countdown = document.getElementById("countdown");
  var go = document.getElementById("continue");
  if (!countdown || !go) {
    return;
  }
  var total = Number(countdown.getAttribute("data-seconds"));
  var left = total;
  var shown = document.getElementById("left");
  var bar = document.getElementById("bar");
  var timer = window.setInterval(function () {
    left -= 1;
    if (left <= 0) {
      window.clearInterval(timer);
      window.location.assign(go.getAttribute("href"));
      return;
    }
    shown.textContent = String(left);
    bar.style.width = String((100 * left) / total) + "%";
  }, 1000);
  document.getElementById("stay").addEventListener("click", function (e) {
    e.preventDefault();
    window.clearInterval(timer);
    countdown.hidden = true;
  });
})();
