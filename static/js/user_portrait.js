// 特点画像（测试中）。后端见 user_portrait.py，提示词变量 {{user_portrait}}。
// 独立文件，不依赖 dashboard.js 里的函数；进入「用户画像」页时自动加载。
(function () {
    let current = null;      // GET /api/user-portrait 的结果
    let preview = null;      // generate-preview 的结果
    let editing = null;      // 编辑中的条目副本
    let busy = false;

    const ESC_MAP = {'&': '&' + 'amp;', '<': '&' + 'lt;', '>': '&' + 'gt;', '"': '&' + '#34;', "'": '&' + '#39;'};
    function esc(s) {
        return String(s == null ? '' : s).replace(/[&<>"']/g, ch => ESC_MAP[ch]);
    }
    function $(id) { return document.getElementById(id); }
    function msg(text, type) {
        const el = $('upMsg');
        if (!el) return;
        el.innerHTML = text ? '<div class="msg-box msg-' + (type || 'success') + '">' + esc(text) + '</div>' : '';
    }
    async function api(url, opts) {
        const resp = await fetch(url, opts);
        let data = null;
        try { data = await resp.json(); } catch (e) { throw new Error('HTTP ' + resp.status + '：返回的不是 JSON'); }
        if (!resp.ok || data.status === 'error' || data.error) {
            const err = new Error((data && (data.error || data.message)) || ('HTTP ' + resp.status));
            err.data = data;
            throw err;
        }
        return data;
    }
    function traitHtml(t, extra) {
        return '<div style="padding:10px 12px;border:1px solid var(--border-color);border-radius:10px;margin:8px 0;background:#fff;' + (extra || '') + '">' +
            '<div style="font-weight:800;margin-bottom:4px;">' + esc(t.title) + '</div>' +
            '<div style="font-size:13px;line-height:1.7;white-space:pre-wrap;">' + esc(t.content) + '</div></div>';
    }

    function render() {
        const box = $('upCurrent');
        if (!box) return;
        if (editing) { renderEditor(); return; }
        const traits = (current && current.traits) || [];
        let html = '';
        if (!traits.length) {
            html = '<div style="color:var(--text-muted);font-size:13px;">还没有特点画像。点「从零生成」，旧的标签画像会作为参考材料。</div>';
        } else {
            html = '<div style="font-size:12px;color:var(--text-muted);margin-bottom:4px;">' + traits.length + ' 条 · 更新时间：' + esc((current.updated_at || '').slice(0, 16).replace('T', ' ')) + '</div>' +
                traits.map(t => traitHtml(t)).join('');
        }
        box.innerHTML = html;
        const hasTraits = traits.length > 0;
        ['upUpdateBtn', 'upEditBtn', 'upDeleteBtn'].forEach(id => { const b = $(id); if (b) b.disabled = !hasTraits; });
    }

    async function load() {
        try {
            const data = await api('/api/user-portrait?character_id=default');
            current = data.status === 'ok' ? data : null;
            fillDefaultRange(data);
        } catch (e) {
            current = null;
            msg('特点画像加载失败：' + e.message, 'error');
        }
        render();
    }

    // ---------- 日期范围 ----------
    function addDays(iso, n) {
        const d = new Date(iso + 'T00:00:00Z');
        d.setUTCDate(d.getUTCDate() + n);
        return d.toISOString().slice(0, 10);
    }

    // 只在输入框还空着时填默认值：开始 = 上次范围的次日（没有就是 30 天前），结束 = 今天
    function fillDefaultRange(data) {
        const s = $('upStart'), e = $('upEnd');
        if (!s || !e) return;
        const today = (data && data.today) || new Date().toISOString().slice(0, 10);
        if (!e.value) e.value = today;
        if (!s.value) {
            const last = data && data.last_range_end;
            if (last) {
                const next = addDays(last, 1);
                s.value = next <= today ? next : today;
            } else {
                s.value = addDays(today, -29);
            }
        }
        const hint = $('upRangeHint');
        if (hint) hint.textContent = (data && data.last_range_start) ? ('上次用的范围：' + data.last_range_start + ' ~ ' + data.last_range_end) : '';
    }

    function rangeParams() {
        const s = ($('upStart') || {}).value || '';
        const e = ($('upEnd') || {}).value || '';
        if (!s || !e) throw new Error('请选择开始和结束日期');
        if (s > e) throw new Error('开始日期不能晚于结束日期');
        const old = $('upIncludeOld');
        return {start: s, end: e, include_old: !!(old && old.checked)};
    }

    async function showMaterials() {
        if (busy) return;
        let range;
        try { range = rangeParams(); } catch (e) { msg(e.message, 'error'); return; }
        busy = true;
        const pv = $('upPreview');
        pv.style.display = 'block';
        pv.innerHTML = '<div style="color:var(--text-muted);">正在收集材料...</div>';
        try {
            const d = await api('/api/user-portrait/materials', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(Object.assign({character_id: 'default'}, range))});
            const st = d.stats || {};
            const over = d.material_chars > d.prompt_max_chars;
            pv.innerHTML = '<div style="font-weight:800;">材料预览 <span style="font-weight:400;font-size:12px;color:var(--text-muted);">' + esc(d.range_start) + ' ~ ' + esc(d.range_end) + '</span></div>' +
                '<div style="font-size:12px;color:' + (over ? '#dc2626' : 'var(--text-muted)') + ';margin:2px 0 8px;">日印象 ' + (st.daily_count || 0) + ' 篇 · 有聊天 ' + (st.chat_days || 0) + ' 天 · 用户消息 ' + (st.user_messages || 0) + ' 条 · 角色回复 ' + (st.assistant_messages || 0) + ' 条' +
                (st.used_old_impression ? ' · 附了旧画像' : '') + ' · 共 ' + (d.material_chars || 0) + ' 字' + (over ? '（超过上限 ' + d.prompt_max_chars + '，请缩短范围）' : '') + '</div>' +
                '<pre style="white-space:pre-wrap;font-size:12px;line-height:1.6;max-height:420px;overflow:auto;background:#fff;border:1px solid var(--border-color);border-radius:8px;padding:10px;">' + esc(d.material_text) + '</pre>' +
                '<div style="text-align:right;margin-top:8px;"><button class="btn btn-sm" onclick="upClosePreview()">关闭</button></div>';
        } catch (e) {
            pv.innerHTML = '<div class="msg-box msg-error">收集材料失败：' + esc(e.message) + '</div><div style="text-align:right;margin-top:8px;"><button class="btn btn-sm" onclick="upClosePreview()">关闭</button></div>';
        } finally {
            busy = false;
        }
    }

    // ---------- 生成预览 ----------
    const OP_LABEL = {add: '新增', edit: '修改', merge: '合并', delete: '删除'};
    const OP_COLOR = {add: '#059669', edit: '#2563eb', merge: '#7c3aed', delete: '#dc2626'};

    async function generate(mode) {
        if (busy) return;
        let range;
        try { range = rangeParams(); } catch (e) { msg(e.message, 'error'); return; }
        if (mode === 'initial' && current && (current.traits || []).length &&
            !confirm('从零生成会在预览确认后替换现有的 ' + current.traits.length + ' 条特点。继续吗？')) return;
        busy = true;
        preview = null;
        const pv = $('upPreview');
        pv.style.display = 'block';
        pv.innerHTML = '<div style="color:var(--text-muted);">正在' + (mode === 'update' ? '更新' : '生成') + '特点画像（' + esc(range.start) + ' ~ ' + esc(range.end) + '），可能要等一两分钟...</div>';
        try {
            preview = await api('/api/user-portrait/generate-preview', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify(Object.assign({character_id: 'default', mode: mode}, range))});
            renderPreview();
        } catch (e) {
            const raw = e.data && e.data.raw;
            pv.innerHTML = '<div class="msg-box msg-error">生成失败：' + esc(e.message) + '</div>' +
                (raw ? '<details style="margin-top:6px;"><summary style="cursor:pointer;font-size:12px;color:var(--text-muted);">模型返回原文</summary><pre style="white-space:pre-wrap;font-size:12px;max-height:240px;overflow:auto;">' + esc(raw) + '</pre></details>' : '') +
                '<div style="text-align:right;margin-top:8px;"><button class="btn btn-sm" onclick="upClosePreview()">关闭</button></div>';
        } finally {
            busy = false;
        }
    }

    function renderPreview() {
        const pv = $('upPreview');
        if (!pv || !preview) return;
        const byId = {};
        (preview.current_traits || []).forEach(t => { byId[t.id] = t; });
        const ms = preview.material_summary || {};
        const clueText = {};
        (preview.clues || []).forEach(c => { clueText[c.id] = c.text; });
        let html = '<div style="font-weight:800;">' + (preview.mode === 'update' ? '更新预览' : '生成预览') +
            ' <span style="font-weight:400;font-size:12px;color:var(--text-muted);">' + esc(preview.range_start || '') + ' ~ ' + esc(preview.range_end || '') + '</span></div>' +
            '<div style="font-size:12px;color:var(--text-muted);margin:2px 0 8px;">日印象 ' + (ms.daily_count || 0) + ' 篇 · 用户消息 ' + (ms.user_messages || 0) + ' 条 · 角色回复 ' + (ms.assistant_messages || 0) + ' 条' +
            (ms.used_old_impression ? ' · 附了旧画像' : '') + ' · 提示词 ' + (ms.prompt_chars || 0) + ' 字</div>';
        (preview.warnings || []).forEach(w => { html += '<div style="font-size:12px;color:#b45309;">⚠ ' + esc(w) + '</div>'; });
        if ((preview.clues || []).length) {
            html += '<details style="margin:8px 0;"><summary style="cursor:pointer;font-size:13px;font-weight:700;">模型找到的线索（' + preview.clues.length + ' 条，不保存）</summary>' +
                '<div style="font-size:12px;line-height:1.7;padding:6px 10px;background:#fff;border:1px solid var(--border-color);border-radius:8px;margin-top:4px;">' +
                preview.clues.map(c => '<div><b>' + esc(c.id) + '</b> ' + esc(c.text) + '</div>').join('') + '</div></details>';
        }
        const clueLine = o => {
            const refs = o.clues || [];
            if (!refs.length) return '<div style="font-size:12px;color:#b45309;margin-top:4px;">没有引用线索</div>';
            return '<div style="font-size:12px;color:var(--text-muted);margin-top:4px;line-height:1.6;">依据：' +
                refs.map(id => '<b>' + esc(id) + '</b> ' + esc(clueText[id] || '')).join('；') + '</div>';
        };
        const ops = preview.ops || [];
        if (!ops.length) {
            html += '<div style="margin:10px 0;color:var(--text-muted);">模型认为没有需要改的地方。</div>' +
                '<div style="text-align:right;"><button class="btn btn-sm" onclick="upClosePreview()">关闭</button></div>';
            pv.innerHTML = html;
            return;
        }
        if (preview.mode === 'update') html += '<div style="font-size:12px;color:var(--text-muted);">没列出来的条目保持原样。</div>';
        ops.forEach((o, i) => {
            const color = OP_COLOR[o.op] || '#475569';
            let body = '';
            if (o.op === 'delete') {
                const t = byId[o.id];
                body = t ? traitHtml(t, 'opacity:.6;text-decoration:line-through;') : '';
            } else {
                if (o.op === 'edit' && byId[o.id]) {
                    body += '<details style="margin:4px 0;"><summary style="cursor:pointer;font-size:12px;color:var(--text-muted);">改之前</summary>' + traitHtml(byId[o.id], 'opacity:.7;') + '</details>';
                }
                if (o.op === 'merge') {
                    body += '<div style="font-size:12px;color:var(--text-muted);">合并：' + (o.ids || []).map(id => '「' + esc((byId[id] || {}).title || id) + '」').join(' + ') + '</div>';
                }
                body += traitHtml(o, 'border-color:' + color + ';');
                body += clueLine(o);
            }
            html += '<label style="display:block;margin:10px 0;padding:10px;border:1px dashed ' + color + ';border-radius:10px;cursor:pointer;">' +
                '<div style="display:flex;gap:8px;align-items:center;"><input type="checkbox" class="up-op-check" data-index="' + i + '" checked>' +
                '<span style="font-weight:800;color:' + color + ';">' + (OP_LABEL[o.op] || esc(o.op)) + '</span>' +
                (o.reason ? '<span style="font-size:12px;color:var(--text-muted);">' + esc(o.reason) + '</span>' : '') + '</div>' +
                body + '</label>';
        });
        html += '<div style="display:flex;gap:8px;justify-content:flex-end;margin-top:10px;">' +
            '<button class="btn btn-sm" onclick="upClosePreview()">取消</button>' +
            '<button class="btn btn-primary btn-sm" onclick="upConfirmPreview()">确认保存</button></div>';
        pv.innerHTML = html;
    }

    function selectedOps() {
        const picked = new Set();
        document.querySelectorAll('.up-op-check').forEach(cb => { if (cb.checked) picked.add(Number(cb.dataset.index)); });
        return (preview.ops || []).filter((o, i) => picked.has(i));
    }

    async function confirmPreview() {
        if (!preview || busy) return;
        const ops = selectedOps();
        if (preview.mode === 'initial' && !ops.length) { msg('至少要勾选一条', 'error'); return; }
        if (preview.mode === 'update' && !ops.length) { closePreview(); msg('没有勾选任何改动，画像没变', 'info'); return; }
        busy = true;
        try {
            await api('/api/user-portrait/confirm', {method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({
                character_id: 'default', mode: preview.mode, ops: ops,
                base_revision: preview.base_revision || 0,
                range_start: preview.range_start || null, range_end: preview.range_end || null
            })});
            // 下次默认从这次范围的次日开始
            const s = $('upStart'), e = $('upEnd');
            if (s && preview.range_end) s.value = '';
            if (e) e.value = '';
            closePreview();
            await load();
            msg('特点画像已保存');
        } catch (e) {
            msg('保存失败：' + e.message, 'error');
        } finally {
            busy = false;
        }
    }

    function closePreview() {
        preview = null;
        const pv = $('upPreview');
        if (pv) { pv.style.display = 'none'; pv.innerHTML = ''; }
    }

    // ---------- 手动编辑 ----------
    function openEditor() {
        if (!current || !(current.traits || []).length) return;
        editing = current.traits.map(t => ({id: t.id, title: t.title, content: t.content}));
        renderEditor();
    }

    function syncEditing() {
        if (!editing) return;
        editing.forEach((t, i) => {
            const ti = document.querySelector('.up-edit-title[data-index="' + i + '"]');
            const ci = document.querySelector('.up-edit-content[data-index="' + i + '"]');
            if (ti) t.title = ti.value;
            if (ci) t.content = ci.value;
        });
    }

    function renderEditor() {
        const box = $('upCurrent');
        if (!box || !editing) return;
        const inputStyle = 'width:100%;box-sizing:border-box;padding:6px 8px;border:1px solid var(--border-color);border-radius:6px;font-size:13px;';
        let html = '';
        editing.forEach((t, i) => {
            html += '<div style="padding:10px;border:1px solid var(--border-color);border-radius:10px;margin:8px 0;background:#fff;">' +
                '<div style="display:flex;gap:6px;align-items:center;margin-bottom:6px;">' +
                '<input class="up-edit-title" data-index="' + i + '" value="' + esc(t.title) + '" placeholder="特点名" style="' + inputStyle + 'font-weight:700;">' +
                '<button class="btn btn-sm" onclick="upEditMove(' + i + ',-1)" title="上移">↑</button>' +
                '<button class="btn btn-sm" onclick="upEditMove(' + i + ',1)" title="下移">↓</button>' +
                '<button class="btn btn-sm" onclick="upEditRemove(' + i + ')" style="color:#dc2626;border-color:#dc2626;">删</button></div>' +
                '<textarea class="up-edit-content" data-index="' + i + '" rows="5" style="' + inputStyle + 'line-height:1.6;">' + esc(t.content) + '</textarea></div>';
        });
        html += '<div style="display:flex;gap:8px;justify-content:space-between;flex-wrap:wrap;margin-top:8px;">' +
            '<button class="btn btn-sm" onclick="upEditAdd()">+ 加一条</button>' +
            '<div style="display:flex;gap:8px;"><button class="btn btn-sm" onclick="upEditCancel()">取消</button>' +
            '<button class="btn btn-primary btn-sm" onclick="upEditSave()">保存</button></div></div>';
        box.innerHTML = html;
    }

    function editMove(i, d) {
        syncEditing();
        const j = i + d;
        if (j < 0 || j >= editing.length) return;
        const t = editing[i]; editing[i] = editing[j]; editing[j] = t;
        renderEditor();
    }
    function editRemove(i) { syncEditing(); editing.splice(i, 1); renderEditor(); }
    function editAdd() { syncEditing(); editing.push({id: '', title: '', content: ''}); renderEditor(); }
    function editCancel() { editing = null; render(); }

    async function editSave() {
        if (busy) return;
        syncEditing();
        const traits = editing.filter(t => String(t.content || '').trim());
        if (!traits.length && !confirm('所有条目都是空的，保存后画像会被清空。继续吗？')) return;
        busy = true;
        try {
            await api('/api/user-portrait', {method: 'PUT', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({
                character_id: 'default', base_revision: (current && current.revision) || 0, traits: traits
            })});
            editing = null;
            await load();
            msg('特点画像已保存');
        } catch (e) {
            msg('保存失败：' + e.message, 'error');
        } finally {
            busy = false;
        }
    }

    async function del() {
        if (!confirm('删除特点画像？旧的标签画像不受影响。')) return;
        try {
            await api('/api/user-portrait?character_id=default', {method: 'DELETE'});
            closePreview(); editing = null;
            await load();
            msg('特点画像已删除');
        } catch (e) { msg('删除失败：' + e.message, 'error'); }
    }

    window.upGenerate = generate;
    window.upShowMaterials = showMaterials;
    window.upConfirmPreview = confirmPreview;
    window.upClosePreview = closePreview;
    window.upOpenEditor = openEditor;
    window.upEditMove = editMove;
    window.upEditRemove = editRemove;
    window.upEditAdd = editAdd;
    window.upEditCancel = editCancel;
    window.upEditSave = editSave;
    window.upDelete = del;
    window.loadUserPortrait = load;
    // 测试用
    window.__up = {
        get: () => ({current, preview, editing}),
        set: (k, v) => { if (k === 'current') current = v; if (k === 'preview') preview = v; if (k === 'editing') editing = v; },
        renderPreview, render, selectedOps, syncEditing
    };

    let loaded = false;
    function maybeLoad() { if (!loaded && $('upCurrent')) { loaded = true; load(); } }
    document.addEventListener('click', function (ev) {
        const nav = ev.target.closest && ev.target.closest('[data-section="user-impression"]');
        if (nav) setTimeout(maybeLoad, 0);
    });
    document.addEventListener('DOMContentLoaded', function () {
        const sec = $('section-user-impression');
        if (sec && sec.classList.contains('active')) maybeLoad();
    });
})();
