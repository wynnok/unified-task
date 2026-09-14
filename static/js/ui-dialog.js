/* ============================================
   统一确认 / 提示弹窗组件（ui-dialog）
   替代原生 confirm() / alert()，与整体设计风格保持一致：
   - UIDialog.confirm({ title, message, okText, cancelText, danger }) -> Promise<boolean>
   - UIDialog.alert({ title, message, okText, icon })                -> Promise<boolean>
   - 表单声明 data-confirm="提示文案" 可自动接入确认流程，
     可选 data-confirm-title / data-confirm-ok / data-confirm-danger="0"
   ============================================ */
(function () {
    'use strict';

    var overlay = null;
    var els = null;
    var pendingResolve = null;
    var lastFocused = null;

    function ensureDom() {
        if (overlay) { return; }

        overlay = document.createElement('div');
        overlay.className = 'ui-dialog-overlay';
        overlay.hidden = true;
        overlay.innerHTML =
            '<div class="ui-dialog" role="alertdialog" aria-modal="true" aria-labelledby="ui-dialog-title" aria-describedby="ui-dialog-message">' +
            '    <div class="ui-dialog-icon" aria-hidden="true"><i class="ph"></i></div>' +
            '    <h3 class="ui-dialog-title" id="ui-dialog-title"></h3>' +
            '    <p class="ui-dialog-message" id="ui-dialog-message"></p>' +
            '    <div class="ui-dialog-actions">' +
            '        <button type="button" class="btn-link ui-dialog-cancel">取消</button>' +
            '        <button type="button" class="btn btn-primary ui-dialog-ok">确定</button>' +
            '    </div>' +
            '</div>';
        document.body.appendChild(overlay);

        els = {
            dialog: overlay.querySelector('.ui-dialog'),
            icon: overlay.querySelector('.ui-dialog-icon i'),
            title: overlay.querySelector('.ui-dialog-title'),
            message: overlay.querySelector('.ui-dialog-message'),
            cancel: overlay.querySelector('.ui-dialog-cancel'),
            ok: overlay.querySelector('.ui-dialog-ok')
        };

        els.cancel.addEventListener('click', function () { finish(false); });
        els.ok.addEventListener('click', function () { finish(true); });

        overlay.addEventListener('click', function (event) {
            if (event.target === overlay && !els.cancel.hidden) { finish(false); }
        });

        overlay.addEventListener('keydown', function (event) {
            if (event.key === 'Escape') {
                event.preventDefault();
                event.stopPropagation();
                // 单按钮提示按 Esc 视为已知悉；双按钮确认按 Esc 视为取消
                finish(els.cancel.hidden);
                return;
            }
            if (event.key === 'Tab') {
                // 焦点只在「取消 / 确定」两个按钮间循环
                var focusables = els.cancel.hidden ? [els.ok] : [els.cancel, els.ok];
                var index = focusables.indexOf(document.activeElement);
                event.preventDefault();
                index = event.shiftKey
                    ? (index <= 0 ? focusables.length - 1 : index - 1)
                    : (index === focusables.length - 1 || index < 0 ? 0 : index + 1);
                focusables[index].focus();
            }
        });
    }

    function finish(result) {
        if (!pendingResolve) { return; }
        var resolve = pendingResolve;
        pendingResolve = null;
        overlay.hidden = true;
        document.body.classList.remove('ui-dialog-open');
        if (lastFocused && document.contains(lastFocused)) {
            lastFocused.focus({ preventScroll: true });
        }
        lastFocused = null;
        resolve(result);
    }

    function show(options) {
        ensureDom();
        if (pendingResolve) { finish(false); } // 上一个弹窗尚未关闭时按取消处理

        var isAlert = !options.showCancel;
        els.title.textContent = options.title || (isAlert ? '提示' : '请确认操作');
        els.message.textContent = options.message || '';
        els.message.hidden = options.message === '';
        els.ok.textContent = options.okText || (isAlert ? '知道了' : '确定');
        els.ok.className = 'btn ' + (options.danger ? 'btn-danger' : 'btn-primary') + ' ui-dialog-ok';
        els.cancel.hidden = isAlert;
        els.cancel.textContent = options.cancelText || '取消';

        var icon = options.icon || (options.danger ? 'ph-warning' : (isAlert ? 'ph-info' : 'ph-question'));
        els.icon.className = 'ph ' + icon;
        overlay.classList.toggle('is-danger', !!options.danger);

        return new Promise(function (resolve) {
            pendingResolve = resolve;

            lastFocused = document.activeElement;
            overlay.hidden = false;
            document.body.classList.add('ui-dialog-open');
            // 危险操作（删除）默认聚焦「取消」，普通确认聚焦「确定」
            (options.danger && !isAlert ? els.cancel : els.ok).focus({ preventScroll: true });
        });
    }

    window.UIDialog = {
        confirm: function (options) {
            options = options || {};
            options.showCancel = true;
            return show(options);
        },
        alert: function (options) {
            options = options || {};
            options.showCancel = false;
            return show(options);
        }
    };

    // 提交中的表单：标记期间拦截后续 submit（按钮同时会被 CSS 置灰）。
    // 4 秒后自动解除，避免请求异常中断（如网络错误停在原页）时表单永久锁死
    function markSubmitting(form) {
        form.setAttribute('data-ui-submitting', '1');
        setTimeout(function () {
            form.removeAttribute('data-ui-submitting');
        }, 4000);
    }

    // 带 data-confirm 的表单：首次提交弹确认框，确认后放行；
    // 其余表单做双击防重复提交（纯前端局部刷新的表单用 data-no-guard 声明跳过）
    document.addEventListener('submit', function (event) {
        var form = event.target;
        if (!form || !form.hasAttribute) { return; }

        if (form.hasAttribute('data-confirm')) {
            // 确认后的正式提交在防抖窗口内同样拦截（键盘触发不走按钮的 CSS 屏蔽）
            if (form.getAttribute('data-ui-submitting') === '1') {
                event.preventDefault();
                return;
            }
            if (form.getAttribute('data-confirm-passed') === '1') {
                form.removeAttribute('data-confirm-passed');
                markSubmitting(form);
                return;
            }
            event.preventDefault();
            UIDialog.confirm({
                title: form.getAttribute('data-confirm-title') || '请确认操作',
                message: form.getAttribute('data-confirm'),
                okText: form.getAttribute('data-confirm-ok') || '确定',
                danger: form.getAttribute('data-confirm-danger') !== '0'
            }).then(function (ok) {
                if (!ok) { return; }
                form.setAttribute('data-confirm-passed', '1');
                if (typeof form.requestSubmit === 'function') {
                    form.requestSubmit(); // 走原生提交校验，会再次触发 submit 事件并由标记放行
                } else {
                    form.submit();
                }
            });
            return;
        }

        if (form.hasAttribute('data-no-guard')) { return; }
        if (form.getAttribute('data-ui-submitting') === '1') {
            event.preventDefault();
            return;
        }
        markSubmitting(form);
    });
})();
