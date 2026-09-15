/* ============================================
   全局界面增强（app.js）
   - Flash 提示：手动关闭 + 成功类自动消失
   - 密码输入框显隐切换（.password-field 通用）
   - 模态框打开时的 Tab 焦点圈定
   ============================================ */
(function () {
    'use strict';

    function dismissFlash(el) {
        if (el.dataset.dismissed) { return; }
        el.dataset.dismissed = '1';
        el.classList.add('is-leaving');
        setTimeout(function () { el.remove(); }, 220);
    }

    function initFlashes(root) {
        Array.prototype.forEach.call((root || document).querySelectorAll('.flash'), function (el) {
            if (el.dataset.flashBound) { return; }
            el.dataset.flashBound = '1';
            var close = el.querySelector('.flash-close');
            if (close) {
                close.addEventListener('click', function () { dismissFlash(el); });
            }
            // 错误与警告保持常驻，成功提示 5 秒后自动消失
            if (!el.classList.contains('error') && !el.classList.contains('warning')) {
                setTimeout(function () { dismissFlash(el); }, 5000);
            }
        });
    }

    function initPasswordToggles(root) {
        Array.prototype.forEach.call((root || document).querySelectorAll('.password-field'), function (field) {
            var input = field.querySelector('input');
            var button = field.querySelector('.password-toggle');
            if (!input || !button || button.dataset.toggleBound) { return; }
            button.dataset.toggleBound = '1';
            button.addEventListener('click', function () {
                var showing = input.type === 'text';
                input.type = showing ? 'password' : 'text';
                button.innerHTML = '<i class="ph ph-' + (showing ? 'eye' : 'eye-slash') + '"></i>';
                button.setAttribute('aria-label', showing ? '显示密码' : '隐藏密码');
            });
        });
    }

    function initModalFocusTrap() {
        document.addEventListener('keydown', function (event) {
            if (event.key !== 'Tab') { return; }
            // 自定义下拉面板与确认弹窗各自管理焦点，不抢
            if (document.querySelector('.ui-select-menu:not([hidden])')) { return; }
            if (document.querySelector('.ui-dialog-overlay:not([hidden])')) { return; }
            var modal = document.querySelector('.modal-overlay.is-open');
            if (!modal) { return; }
            var focusables = modal.querySelectorAll(
                'button, [href], input, select, textarea, [tabindex]:not([tabindex="-1"])'
            );
            var visible = Array.prototype.filter.call(focusables, function (el) {
                return !el.disabled && el.offsetParent !== null;
            });
            if (!visible.length) { return; }
            var first = visible[0];
            var last = visible[visible.length - 1];
            if (!modal.contains(document.activeElement)) {
                event.preventDefault();
                first.focus();
            } else if (event.shiftKey && document.activeElement === first) {
                event.preventDefault();
                last.focus();
            } else if (!event.shiftKey && document.activeElement === last) {
                event.preventDefault();
                first.focus();
            }
        });
    }

    function init() {
        initFlashes(document);
        initPasswordToggles(document);
        initModalFocusTrap();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
