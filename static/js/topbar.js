(() => {
  function $(id) { return document.getElementById(id); }

  // --- config from your base ---
  const urlTemplate = (window.__OFFICE_SUMMARY_URL_TEMPLATE__) || null;
  // Если хочешь — можно в base положить window.__OFFICE_SUMMARY_URL_TEMPLATE__ = "{% url 'contact_summary' 0 %}";
  // Но мы обойдемся без него: возьмем template прямо из data-атрибута, если добавишь.

  const cityBtn   = $("topbarCityBtn");
  const menu      = $("topbarOfficeMenu");
  const cityText  = $("topbar-city-text");

  const phoneText = $("topbar-phone-text");
  const phoneLink = $("topbar-phone-link");

  const userBtn   = $("topbarUserBtn");
  const userMenu  = $("topbarUserMenu");
  const userWrap  = $("topbarUser");

  const openSearch  = $("topbarSearchOpen");
  const overlay     = $("topbarSearchOverlay");
  const closeSearch = $("topbarSearchClose");
  const inputMobile = $("topbarSearchInputMobile");

  if (!cityBtn || !menu) {
    // топбар не на странице — тихо выходим
    return;
  }

  // ---------- helpers ----------
  function setCookie(name, value, days) {
    const d = new Date();
    d.setTime(d.getTime() + (days * 24 * 60 * 60 * 1000));
    document.cookie = name + "=" + encodeURIComponent(value) + ";expires=" + d.toUTCString() + ";path=/";
  }

  function telHref(str) {
    return "tel:" + (str || "").replace(/[^\d+]/g, "");
  }

  // ПОРТАЛ: переносим меню в body и позиционируем fixed,
  // чтобы его никогда не резало и не прятало под полосой.
  function portalize(menuEl) {
    if (!menuEl || menuEl.dataset.portalized === "1") return;

    const placeholder = document.createComment("menu-placeholder");
    menuEl.parentNode.insertBefore(placeholder, menuEl);

    document.body.appendChild(menuEl);
    menuEl.dataset.portalized = "1";
    menuEl._placeholder = placeholder;
  }

  function positionMenu(menuEl, anchorEl, alignRight = false) {
    if (!menuEl || !anchorEl) return;

    const r = anchorEl.getBoundingClientRect();
    const gap = 8;

    menuEl.style.position = "fixed";
    menuEl.style.top = `${Math.round(r.bottom + gap)}px`;

    // ширина от кнопки, но с минимумом
    const minW = Math.max(280, Math.round(r.width));
    menuEl.style.minWidth = `${minW}px`;

    if (alignRight) {
      const right = Math.round(window.innerWidth - r.right);
      menuEl.style.right = `${Math.max(12, right)}px`;
      menuEl.style.left = "auto";
    } else {
      const left = Math.round(r.left);
      menuEl.style.left = `${Math.max(12, left)}px`;
      menuEl.style.right = "auto";
    }

    // защита от выхода за экран справа
    const mr = menuEl.getBoundingClientRect();
    if (mr.right > window.innerWidth - 12) {
      const shift = mr.right - (window.innerWidth - 12);
      const curLeft = parseInt(menuEl.style.left || "12", 10);
      menuEl.style.left = `${Math.max(12, curLeft - shift)}px`;
    }
  }

  function openMenu(menuEl, anchorEl, alignRight = false) {
    portalize(menuEl);
    positionMenu(menuEl, anchorEl, alignRight);
    menuEl.classList.add("is-open");
    menuEl.setAttribute("aria-hidden", "false");
  }

  function closeMenu(menuEl) {
    if (!menuEl) return;
    menuEl.classList.remove("is-open");
    menuEl.setAttribute("aria-hidden", "true");
  }

  function toggleMenu(menuEl, anchorEl, alignRight = false) {
    if (!menuEl) return;
    if (menuEl.classList.contains("is-open")) closeMenu(menuEl);
    else openMenu(menuEl, anchorEl, alignRight);
  }

  // ---------- office list ----------
  cityBtn.addEventListener("click", (e) => {
    e.preventDefault();
    e.stopPropagation();
    toggleMenu(menu, cityBtn, false);
    closeMenu(userMenu);
  });

  menu.addEventListener("click", async (e) => {
    const a = e.target.closest("[data-office-id]");
    if (!a) return;

    e.preventDefault();

    const id   = a.getAttribute("data-office-id");
    const city = a.getAttribute("data-office-city") || a.getAttribute("data-office-name") || "Офисы";

    if (cityText) cityText.textContent = city;

    // быстрый fallback из dataset
    const fallbackPhone = (a.getAttribute("data-office-phone") || "").trim();

    try {
      // если у тебя есть endpoint contact_summary — лучше тянуть его
      // Мы попробуем вычислить URL из старого шаблона: "/contacts/summary/<id>/" ты уже делал через {% url 'contact_summary' 0 %}
      // Поэтому: в base можно добавить data-summary-template на body, но пока оставим простой fetch по текущему паттерну:
      const guessTemplate = a.getAttribute("data-summary-url-template"); // опционально
      const template = guessTemplate || (window.__OFFICE_SUMMARY_URL_TEMPLATE__ || null);

      if (template) {
        const url = template.replace("/0/", `/${id}/`);
        const resp = await fetch(url, { headers: { "X-Requested-With": "XMLHttpRequest" } });
        if (!resp.ok) throw new Error("HTTP " + resp.status);
        const data = await resp.json();

        const phone = (data.phone || "").trim();
        if (phone) {
          phoneText.textContent = phone;
          phoneLink.setAttribute("href", telHref(phone));
        }
      } else if (fallbackPhone) {
        phoneText.textContent = fallbackPhone;
        phoneLink.setAttribute("href", telHref(fallbackPhone));
      }

      setCookie("office_id", id, 365);
    } catch (_) {
      if (fallbackPhone) {
        phoneText.textContent = fallbackPhone;
        phoneLink.setAttribute("href", telHref(fallbackPhone));
      }
      setCookie("office_id", id, 365);
    }

    closeMenu(menu);
  });

  // ---------- user menu ----------
  if (userBtn && userMenu) {
    // hover on desktop
    if (userWrap) {
      userWrap.addEventListener("mouseenter", () => {
        if (window.matchMedia("(hover: hover)").matches) openMenu(userMenu, userBtn, true);
      });
      userWrap.addEventListener("mouseleave", () => {
        if (window.matchMedia("(hover: hover)").matches) closeMenu(userMenu);
      });
    }

    // click toggle (mobile)
    userBtn.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      toggleMenu(userMenu, userBtn, true);
      closeMenu(menu);
    });
  }

  // ---------- mobile search overlay ----------
  function openS() {
    if (!overlay) return;
    overlay.classList.add("is-open");
    overlay.setAttribute("aria-hidden", "false");
    setTimeout(() => { try { inputMobile && inputMobile.focus(); } catch(_) {} }, 0);
  }
  function closeS() {
    if (!overlay) return;
    overlay.classList.remove("is-open");
    overlay.setAttribute("aria-hidden", "true");
  }

  openSearch && openSearch.addEventListener("click", (e) => { e.preventDefault(); openS(); });
  closeSearch && closeSearch.addEventListener("click", (e) => { e.preventDefault(); closeS(); });
  overlay && overlay.addEventListener("click", (e) => { if (e.target === overlay) closeS(); });

  // ---------- global close / reposition ----------
  document.addEventListener("click", () => {
    closeMenu(menu);
    closeMenu(userMenu);
  });

  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") {
      closeMenu(menu);
      closeMenu(userMenu);
      closeS();
    }
  });

  window.addEventListener("scroll", () => {
    if (menu.classList.contains("is-open")) positionMenu(menu, cityBtn, false);
    if (userMenu && userMenu.classList.contains("is-open")) positionMenu(userMenu, userBtn, true);
  }, { passive: true });

  window.addEventListener("resize", () => {
    if (menu.classList.contains("is-open")) positionMenu(menu, cityBtn, false);
    if (userMenu && userMenu.classList.contains("is-open")) positionMenu(userMenu, userBtn, true);
  });
})();
