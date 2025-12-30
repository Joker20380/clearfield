// auth_modals.js — Bootstrap4 modal loader for django-allauth pages
// Loads /accounts/login/?modal=1 and /accounts/logout/?modal=1 into modal-body
// Submits forms via fetch: on success reload current page; on errors re-render inner HTML.

(() => {
  function getCookie(name) {
    const m = document.cookie.match("(^|;)\\s*" + name + "\\s*=\\s*([^;]+)");
    return m ? m.pop() : "";
  }

  function csrfHeader() {
    const token = getCookie("csrftoken");
    return token ? { "X-CSRFToken": token } : {};
  }

  async function fetchHtml(url, options) {
    const resp = await fetch(url, {
      credentials: "same-origin",
      cache: "no-store",
      ...options,
    });
    // fetch follows redirects; resp.redirected tells us it happened
    const html = await resp.text();
    return { resp, html };
  }

  function setModalLoading($modal, isLoading) {
    const $body = $modal.find(".modal-body");
    const $footer = $modal.find(".modal-footer");
    if (isLoading) {
      $body.html(`
        <div style="padding:10px 2px; color: rgba(255,255,255,.88);">
          Загружаю...
        </div>
      `);
      $footer.hide();
    } else {
      $footer.show();
    }
  }

  function wireAjaxSubmit($modal) {
    const $body = $modal.find(".modal-body");
    const form = $body.find("form[data-auth-modal-form='1']").get(0);
    if (!form) return;

    // Prevent double binding
    if (form.__authModalBound) return;
    form.__authModalBound = true;

    form.addEventListener("submit", async (e) => {
      e.preventDefault();

      const action = form.getAttribute("action") || window.location.href;
      const method = (form.getAttribute("method") || "POST").toUpperCase();

      const fd = new FormData(form);

      try {
        const { resp, html } = await fetchHtml(action, {
          method,
          headers: {
            ...csrfHeader(),
          },
          body: fd,
        });

        // Успех: allauth обычно редиректит (resp.redirected == true)
        // или возвращает 200 с другим URL (редко). В обоих случаях — просто перезагрузим текущую.
        if (resp.redirected || (resp.status >= 300 && resp.status < 400)) {
          window.location.reload();
          return;
        }

        // Если сервер вернул снова форму (ошибки) — заменяем содержимое
        // Важно: мы ожидаем partial HTML (modal=1), но даже если пришла полная страница — вытащим #auth-modal-inner если есть
        const temp = document.createElement("div");
        temp.innerHTML = html;

        const inner = temp.querySelector("#auth-modal-inner");
        $body.html(inner ? inner.innerHTML : html);

        wireAjaxSubmit($modal);
      } catch (err) {
        $body.prepend(`
          <div class="alert alert-danger" style="margin-bottom:12px;">
            Ошибка отправки. Проверь соединение и повтори.
          </div>
        `);
      }
    });
  }

  async function openAuthModal(modalId, url) {
    const $modal = window.jQuery ? window.jQuery(modalId) : null;
    if (!$modal || !$modal.length) return;

    setModalLoading($modal, true);
    $modal.modal("show");

    try {
      const targetUrl = url.includes("?") ? `${url}&modal=1` : `${url}?modal=1`;
      const { resp, html } = await fetchHtml(targetUrl, { method: "GET" });

      // Если вдруг редирект (например уже залогинен) — просто обновимся
      if (resp.redirected) {
        window.location.reload();
        return;
      }

      const temp = document.createElement("div");
      temp.innerHTML = html;
      const inner = temp.querySelector("#auth-modal-inner");

      const $body = $modal.find(".modal-body");
      $body.html(inner ? inner.innerHTML : html);

      setModalLoading($modal, false);
      wireAjaxSubmit($modal);

      // focus first input
      setTimeout(() => {
        const inp = $body.find("input:not([type='hidden']),textarea,select").get(0);
        if (inp) inp.focus();
      }, 50);
    } catch (e) {
      $modal.find(".modal-body").html(`
        <div class="alert alert-danger">
          Не удалось загрузить окно. Обнови страницу или попробуй позже.
        </div>
      `);
      setModalLoading($modal, false);
    }
  }

  // Bind clicks
  document.addEventListener("click", (e) => {
    const a = e.target.closest("[data-auth-modal]");
    if (!a) return;

    e.preventDefault();

    const kind = a.getAttribute("data-auth-modal"); // login/logout
    if (kind === "login") {
      openAuthModal("#loginModal", a.getAttribute("href") || "/accounts/login/");
    } else if (kind === "logout") {
      openAuthModal("#logoutModal", a.getAttribute("href") || "/accounts/logout/");
    }
  });
})();
