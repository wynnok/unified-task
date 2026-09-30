/* ============================================
   Webhook 通道管理（设置页）
   - 依据 window.WEBHOOK_TARGETS（模板注入）渲染通道卡片
   - 提交设置表单前把通道列表序列化到 webhook_targets_json
   - 占位符：{{title}}/{{content}}/{{url}}/{{time}}，发送时由后端替换
   ============================================ */
(function () {
    'use strict';

    var methodHints = {
        get: "每项为「参数名=值」，占位符值自动 URL 编码；地址中也可直接使用占位符（路径风格）",
        post_json: "JSON 请求体模板：占位符值按 JSON 字符串转义，渲染后必须为合法 JSON；token 等固定字段直接写在模板里",
        post_form: "每项为「参数名=值」，占位符值自动 URL 编码"
    };
    var methodTemplateLabels = {
        get: "查询参数模板",
        post_json: "JSON 请求体模板",
        post_form: "表单参数模板"
    };
    var methodPlaceholders = {
        get: "title={{title}}&content={{content}}",
        post_json: '{"title": "{{title}}", "msg": "{{content}}", "token": "固定 token", "issecure": "0"}',
        post_form: "title={{title}}&content={{content}}"
    };
    var placeholderLegend = "可用占位符：{{title}} 任务标题 · {{content}} 任务内容 · {{url}} 相关链接 · {{time}} 发送时间，发送时自动替换为实际值";

    var targetList = document.getElementById("webhook-target-list");
    var addTargetButton = document.getElementById("addWebhookTarget");
    var targetsInput = document.getElementById("webhook_targets_json");
    var settingsForm = targetsInput ? targetsInput.closest("form") : null;
    var savedTargets = Array.isArray(window.WEBHOOK_TARGETS) ? window.WEBHOOK_TARGETS : [];

    function updateMethodHint(row) {
        var methodSelect = row.querySelector(".target-method");
        var method = methodSelect.value;
        row.querySelector(".target-template-label").textContent = methodTemplateLabels[method] || "请求模板";
        row.querySelector(".target-template").placeholder = methodPlaceholders[method] || "";
        row.querySelector(".webhook-method-hint").textContent =
            (methodHints[method] || "") + "。" + placeholderLegend;
    }

    function createTargetRow(target) {
        var row = document.createElement("div");
        row.className = "webhook-target-card";
        row.dataset.targetId = target && target.id ? String(target.id) : "";
        row.innerHTML = [
            '<div class="webhook-target-head">',
            '  <label class="webhook-field webhook-field-grow"><span>通道名称</span>',
            '    <input type="text" class="target-name" placeholder="例如：showdoc推送 / ggsuper"></label>',
            '  <label class="webhook-field webhook-field-method"><span>请求方式</span>',
            '    <select class="target-method">',
            '      <option value="get">GET 查询</option>',
            '      <option value="post_json">POST JSON</option>',
            '      <option value="post_form">POST 表单</option>',
            '    </select></label>',
            '  <button type="button" class="btn-link danger-text target-remove" aria-label="删除通道"><i class="ph ph-trash"></i> 删除</button>',
            '</div>',
            '<div class="webhook-target-body">',
            '<label class="webhook-field"><span>Webhook 地址</span>',
            '  <input type="text" class="target-url" placeholder="https://example.com/hook"></label>',
            '<label class="webhook-field"><span class="target-template-label">请求模板</span>',
            '  <textarea class="target-template" rows="3"></textarea></label>',
            '<label class="webhook-field"><span>备注（可选）</span>',
            '  <input type="text" class="target-note" placeholder="选填，方便区分用途"></label>',
            '<div class="webhook-method-hint"></div>',
            '</div>'
        ].join("");

        row.querySelector(".target-name").value = (target && target.name) || "";
        row.querySelector(".target-method").value = (target && target.method) || "get";
        row.querySelector(".target-url").value = (target && target.url) || "";
        row.querySelector(".target-template").value = (target && target.template) || "";
        row.querySelector(".target-note").value = (target && target.note) || "";

        row.querySelector(".target-method").addEventListener("change", function () {
            updateMethodHint(row);
        });
        row.querySelector(".target-remove").addEventListener("click", function () {
            row.remove();
        });

        updateMethodHint(row);
        return row;
    }

    function collectTargets() {
        var targets = [];
        targetList.querySelectorAll(".webhook-target-card").forEach(function (row) {
            var target = {
                id: row.dataset.targetId ? parseInt(row.dataset.targetId, 10) : null,
                name: row.querySelector(".target-name").value.trim(),
                method: row.querySelector(".target-method").value,
                url: row.querySelector(".target-url").value.trim(),
                template: row.querySelector(".target-template").value,
                note: row.querySelector(".target-note").value.trim()
            };
            // 整行都为空的新增行不提交，避免误点“添加通道”后无法保存
            if (!target.id && !target.name && !target.url && !target.template && !target.note) {
                return;
            }
            targets.push(target);
        });
        return targets;
    }

    function init() {
        if (!targetList || !addTargetButton || !targetsInput || !settingsForm) { return; }
        if (savedTargets.length === 0) {
            targetList.appendChild(createTargetRow(null));
        } else {
            savedTargets.forEach(function (target) {
                targetList.appendChild(createTargetRow(target));
            });
        }
        addTargetButton.addEventListener("click", function () {
            targetList.appendChild(createTargetRow(null));
        });
        settingsForm.addEventListener("submit", function () {
            targetsInput.value = JSON.stringify(collectTargets());
        });
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init);
    } else {
        init();
    }
})();
