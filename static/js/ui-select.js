/* ============================================
   统一自定义下拉选择组件（ui-select）
   将原生 <select> 增强为与应用风格一致的下拉：
   - 原生 select 保留在表单中（隐藏）负责取值与提交
   - select.value 程序赋值、表单 reset 会自动同步触发器显示
   - 面板挂在 body 下用 fixed 定位，避免被 overflow 裁剪；空间不足自动上翻
   - 支持键盘导航（方向键 / Enter / Esc / 首字母跳转）与 ARIA listbox 语义
   - 选项可通过 <option data-icon="bell"> 渲染前缀图标
   ============================================ */
(function () {
    'use strict';

    var MENU_GAP = 6;          // 面板与触发器的间距
    var MENU_MAX_HEIGHT = 320; // 面板最大高度
    var MENU_MIN_HEIGHT = 140; // 贴边时的最小可用高度
    var MENU_MIN_WIDTH = 160;  // 面板最小宽度
    var VIEWPORT_MARGIN = 8;   // 面板距视口边缘的最小距离
    var TYPEAHEAD_DELAY = 500; // 首字母跳转的缓冲时间

    var openInstance = null;

    function UiSelect(select) {
        this.select = select;
        this.options = [];
        this.open = false;
        this.activeIndex = -1;
        this.typeahead = '';
        this.typeaheadTimer = null;
        this.valueDescriptor = Object.getOwnPropertyDescriptor(HTMLSelectElement.prototype, 'value');

        this.build();
        this.bindSelect();
        this.renderTrigger();
    }

    UiSelect.prototype.build = function () {
        var select = this.select;
        select.setAttribute('data-ui-select-ready', '1');
        select.classList.add('ui-select-native');

        var wrapper = document.createElement('div');
        wrapper.className = 'ui-select';

        var trigger = document.createElement('button');
        trigger.type = 'button';
        trigger.className = 'ui-select-trigger';
        trigger.setAttribute('aria-haspopup', 'listbox');
        trigger.setAttribute('aria-expanded', 'false');
        // 原生 select 上的无障碍标注转移给触发器（原控件已隐藏）
        var ariaLabel = select.getAttribute('aria-label');
        var ariaLabelledby = select.getAttribute('aria-labelledby');
        if (ariaLabel) {
            trigger.setAttribute('aria-label', ariaLabel);
            select.removeAttribute('aria-label');
        }
        if (ariaLabelledby) {
            trigger.setAttribute('aria-labelledby', ariaLabelledby);
            select.removeAttribute('aria-labelledby');
        }
        // 触发器在 DOM 上位于原生 select 之前：当 select 处于 <label> 内时，
        // 点击标签文本会激活触发器而不是被隐藏的原生控件
        trigger.innerHTML = '<span class="ui-select-label"></span>' +
            '<i class="ph ph-caret-down ui-select-caret" aria-hidden="true"></i>';

        select.parentNode.insertBefore(wrapper, select);
        wrapper.appendChild(trigger);
        wrapper.appendChild(select);

        var menu = document.createElement('div');
        menu.className = 'ui-select-menu';
        menu.setAttribute('role', 'listbox');
        menu.tabIndex = -1;
        menu.hidden = true;
        document.body.appendChild(menu);

        this.wrapper = wrapper;
        this.trigger = trigger;
        this.label = trigger.querySelector('.ui-select-label');
        this.menu = menu;

        this.buildMenu();
    };

    UiSelect.prototype.buildMenu = function () {
        var select = this.select;
        var menu = this.menu;
        var self = this;

        this.options = [];
        menu.innerHTML = '';

        Array.prototype.forEach.call(select.options, function (option, index) {
            var item = document.createElement('button');
            item.type = 'button';
            item.className = 'ui-select-option';
            item.setAttribute('role', 'option');
            item.tabIndex = -1;

            var iconValue = option.getAttribute('data-icon');
            if (iconValue) {
                var icon = document.createElement('i');
                icon.className = 'ph ph-' + iconValue + ' ui-select-option-icon';
                icon.setAttribute('aria-hidden', 'true');
                item.appendChild(icon);
            }

            var text = document.createElement('span');
            text.className = 'ui-select-option-text';
            text.textContent = option.textContent;
            item.appendChild(text);

            var check = document.createElement('i');
            check.className = 'ph ph-check ui-select-option-check';
            check.setAttribute('aria-hidden', 'true');
            item.appendChild(check);

            if (option.disabled) { item.disabled = true; }

            item.addEventListener('click', function () { self.choose(index); });
            menu.appendChild(item);
            self.options.push(item);
        });

        if (!this.options.length) {
            var empty = document.createElement('div');
            empty.className = 'ui-select-empty';
            empty.textContent = '暂无可选项';
            menu.appendChild(empty);
        }
    };

    UiSelect.prototype.renderTrigger = function () {
        var select = this.select;
        var selected = select.selectedIndex >= 0 ? select.options[select.selectedIndex] : null;
        var label = this.label;
        label.innerHTML = '';

        if (selected) {
            var iconValue = selected.getAttribute('data-icon');
            if (iconValue) {
                var icon = document.createElement('i');
                icon.className = 'ph ph-' + iconValue + ' ui-select-trigger-icon';
                icon.setAttribute('aria-hidden', 'true');
                label.appendChild(icon);
            }
            label.appendChild(document.createTextNode(selected.textContent));
        }
        this.wrapper.classList.toggle('is-empty', !selected || selected.textContent === '');
        this.trigger.disabled = select.disabled;
        this.wrapper.classList.toggle('is-disabled', select.disabled);
    };

    // 触发器显示与面板选中态总是一起变化，统一从这里刷新
    UiSelect.prototype.syncUi = function () {
        this.renderTrigger();
        this.syncMenuSelection();
        if (this.select.selectedIndex >= 0) {
            this.wrapper.classList.remove('is-invalid');
        }
    };

    UiSelect.prototype.syncMenuSelection = function () {
        var select = this.select;
        for (var i = 0; i < this.options.length; i++) {
            this.options[i].setAttribute('aria-selected', i === select.selectedIndex ? 'true' : 'false');
        }
    };

    UiSelect.prototype.setActive = function (index) {
        var count = this.options.length;
        if (!count) { this.activeIndex = -1; return; }
        var next = index;
        if (next < 0) { next = count - 1; }
        if (next >= count) { next = 0; }
        if (this.options[next].disabled) {
            var step = index > this.activeIndex ? 1 : -1;
            var probe = next;
            do {
                probe += step;
                if (probe < 0) { probe = count - 1; }
                if (probe >= count) { probe = 0; }
            } while (this.options[probe].disabled && probe !== next);
            next = probe;
        }
        this.activeIndex = next;
        for (var i = 0; i < count; i++) {
            this.options[i].classList.toggle('is-active', i === next);
        }
        this.options[next].scrollIntoView({ block: 'nearest' });
    };

    UiSelect.prototype.position = function () {
        var rect = this.trigger.getBoundingClientRect();
        if (rect.width === 0 && rect.height === 0) {
            // 触发器不可见（如弹窗被关闭）时收起面板
            this.close(false);
            return;
        }
        var menu = this.menu;
        var viewportWidth = window.innerWidth;
        var viewportHeight = window.innerHeight;

        menu.style.maxHeight = MENU_MAX_HEIGHT + 'px';
        var naturalHeight = menu.scrollHeight;

        var spaceBelow = viewportHeight - VIEWPORT_MARGIN - rect.bottom;
        var spaceAbove = rect.top - VIEWPORT_MARGIN;
        var openUp = spaceBelow < Math.min(naturalHeight, MENU_MAX_HEIGHT) && spaceAbove > spaceBelow;
        var maxHeight = Math.min(Math.max(openUp ? spaceAbove : spaceBelow, MENU_MIN_HEIGHT), MENU_MAX_HEIGHT);

        var width = Math.min(Math.max(rect.width, menu.scrollWidth, MENU_MIN_WIDTH), viewportWidth - VIEWPORT_MARGIN * 2);
        var left = Math.max(VIEWPORT_MARGIN, Math.min(rect.left, viewportWidth - VIEWPORT_MARGIN - width));

        menu.style.minWidth = Math.round(Math.max(rect.width, MENU_MIN_WIDTH)) + 'px';
        menu.style.maxWidth = Math.round(viewportWidth - VIEWPORT_MARGIN * 2) + 'px';
        menu.style.maxHeight = Math.round(maxHeight) + 'px';
        menu.style.left = Math.round(left) + 'px';
        if (openUp) {
            menu.style.top = 'auto';
            menu.style.bottom = Math.round(viewportHeight - rect.top + MENU_GAP) + 'px';
        } else {
            menu.style.bottom = 'auto';
            menu.style.top = Math.round(rect.bottom + MENU_GAP) + 'px';
        }
        menu.classList.toggle('is-above', openUp);
    };

    UiSelect.prototype.openMenu = function (activateLast) {
        if (this.open || this.trigger.disabled) { return; }
        if (openInstance && openInstance !== this) { openInstance.close(false); }
        this.open = true;
        openInstance = this;

        this.syncMenuSelection();
        this.menu.hidden = false;
        this.position();
        this.wrapper.classList.add('is-open');
        this.trigger.setAttribute('aria-expanded', 'true');

        var firstIndex = this.select.selectedIndex >= 0 ? this.select.selectedIndex : 0;
        this.setActive(activateLast ? this.options.length - 1 : firstIndex);
        this.menu.focus({ preventScroll: true });

        document.addEventListener('pointerdown', this.handlers.outside, true);
        document.addEventListener('keydown', this.handlers.docKeydown, true);
        window.addEventListener('resize', this.handlers.reposition);
        window.addEventListener('scroll', this.handlers.reposition, true);
    };

    UiSelect.prototype.close = function (focusTrigger) {
        if (!this.open) { return; }
        this.open = false;
        if (openInstance === this) { openInstance = null; }

        this.menu.hidden = true;
        this.wrapper.classList.remove('is-open');
        this.trigger.setAttribute('aria-expanded', 'false');
        this.clearTypeahead();

        document.removeEventListener('pointerdown', this.handlers.outside, true);
        document.removeEventListener('keydown', this.handlers.docKeydown, true);
        window.removeEventListener('resize', this.handlers.reposition);
        window.removeEventListener('scroll', this.handlers.reposition, true);

        if (focusTrigger !== false) { this.trigger.focus({ preventScroll: true }); }
    };

    UiSelect.prototype.choose = function (index) {
        var option = this.select.options[index];
        this.close(true);
        if (!option) { return; }
        var current = this.valueDescriptor.get.call(this.select);
        if (current !== option.value) {
            this.valueDescriptor.set.call(this.select, option.value);
            this.syncUi();
            this.select.dispatchEvent(new Event('change', { bubbles: true }));
        }
    };

    UiSelect.prototype.clearTypeahead = function () {
        this.typeahead = '';
        if (this.typeaheadTimer) {
            clearTimeout(this.typeaheadTimer);
            this.typeaheadTimer = null;
        }
    };

    UiSelect.prototype.applyTypeahead = function (char) {
        var self = this;
        this.typeahead += char.toLowerCase();
        if (this.typeaheadTimer) { clearTimeout(this.typeaheadTimer); }
        this.typeaheadTimer = setTimeout(function () { self.clearTypeahead(); }, TYPEAHEAD_DELAY);

        var needle = this.typeahead;
        var count = this.options.length;
        for (var offset = 1; offset <= count; offset++) {
            var index = (this.activeIndex + offset) % count;
            var text = this.select.options[index] ? this.select.options[index].textContent.toLowerCase() : '';
            if (!this.options[index].disabled && text.indexOf(needle) === 0) {
                this.setActive(index);
                return;
            }
        }
    };

    UiSelect.prototype.bindSelect = function () {
        var self = this;
        var select = this.select;

        this.handlers = {
            outside: function (event) {
                if (!self.wrapper.contains(event.target) && !self.menu.contains(event.target)) {
                    self.close(false);
                }
            },
            docKeydown: function (event) {
                if (event.key === 'Escape') {
                    event.preventDefault();
                    event.stopImmediatePropagation(); // 只收起面板，不再触发页面级 Esc 逻辑
                    self.close(true);
                }
            },
            reposition: function () {
                if (self.open) { self.position(); }
            }
        };

        this.trigger.addEventListener('click', function () {
            if (self.open) { self.close(true); } else { self.openMenu(false); }
        });

        this.trigger.addEventListener('keydown', function (event) {
            if (self.open) { return; }
            if (event.key === 'ArrowDown' || event.key === 'Enter' || event.key === ' ') {
                event.preventDefault();
                self.openMenu(false);
            } else if (event.key === 'ArrowUp') {
                event.preventDefault();
                self.openMenu(true);
            }
        });

        this.menu.addEventListener('keydown', function (event) {
            switch (event.key) {
            case 'ArrowDown':
                event.preventDefault();
                self.setActive(self.activeIndex + 1);
                break;
            case 'ArrowUp':
                event.preventDefault();
                self.setActive(self.activeIndex - 1);
                break;
            case 'Home':
                event.preventDefault();
                self.setActive(0);
                break;
            case 'End':
                event.preventDefault();
                self.setActive(self.options.length - 1);
                break;
            case 'Enter':
            case ' ':
                event.preventDefault();
                self.choose(self.activeIndex);
                break;
            case 'Tab':
                self.close(false);
                break;
            default:
                if (event.key.length === 1 && !event.ctrlKey && !event.metaKey && !event.altKey) {
                    self.applyTypeahead(event.key);
                }
            }
        });

        // 程序赋值（select.value = x）时同步触发器显示
        var descriptor = this.valueDescriptor;
        Object.defineProperty(select, 'value', {
            get: function () { return descriptor.get.call(select); },
            set: function (value) {
                descriptor.set.call(select, value);
                self.syncUi();
            }
        });

        // 表单 reset（含 modalForm.reset()）后值会变化，异步等重置完成再同步
        if (select.form) {
            select.form.addEventListener('reset', function () {
                setTimeout(function () { self.syncUi(); }, 0);
            });
        }

        // 必填校验失败时原生气泡会锚定在隐藏控件上看不见：
        // 拦掉默认气泡，改为高亮触发器并直接展开面板供选择
        select.addEventListener('invalid', function (event) {
            event.preventDefault();
            self.wrapper.classList.add('is-invalid');
            if (!self.open) { self.openMenu(false); }
        });
    };

    function enhanceAll(root) {
        var scope = root || document;
        var created = [];
        var selects = scope.querySelectorAll('select:not([data-ui-select-ready]):not([multiple])');
        Array.prototype.forEach.call(selects, function (select) {
            created.push(new UiSelect(select).select);
        });
        return created;
    }

    window.UISelect = {
        enhanceAll: enhanceAll,
        // 关闭当前展开的下拉（如弹窗关闭时调用，避免面板残留悬浮）
        closeAll: function () {
            if (openInstance) { openInstance.close(false); }
        }
    };

    function init() { enhanceAll(document); }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
