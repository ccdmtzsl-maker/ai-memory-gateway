let _dailyImpressions = [];
let _lastDailyRaw = '';
let _editingDailyIndex = -1;

function _dailyTags(item) {
    return item.tags || item.topics || '';
}

function _renderDailyDetail(item, containerId) {
    const result = document.getElementById(containerId);
    if (!item || !result) return;
    const tags = _dailyTags(item);
    result.innerHTML =
        '<div style="font-size:13px;color:var(--text-muted);margin-bottom:6px;">' +
        escHtml(item.date || '') + (item.mood ? ' · ' + escHtml(item.mood) : '') + '</div>' +
        '<div style="white-space:pre-wrap;line-height:1.7;font-size:15px;">' + escHtml(item.summary || '') + '</div>' +
        (tags ? '<div style="margin-top:12px;color:var(--text-muted);font-size:13px;">标签：' + escHtml(tags) + '</div>' : '');
}

async function _fetchDailyImpressions() {
    const resp = await fetch('/api/daily-impressions?limit=60');
    const text = await resp.text();
    let data = null;
    try {
        data = text ? JSON.parse(text) : {};
    } catch (e) {
        throw new Error('HTTP ' + resp.status + ': ' + text.slice(0, 160));
    }
    if (!resp.ok) throw new Error(data.error || data.message || ('HTTP ' + resp.status));
    if (data.error) throw new Error(data.error);
    _dailyImpressions = data.impressions || [];
    return _dailyImpressions;
}

async function loadDailyImpressions() {
    const list = document.getElementById('dailyImpressionList');
    if (!list) return;
    list.innerHTML = '加载中...';
    try {
        const items = await _fetchDailyImpressions();
        if (!items.length) { list.innerHTML = '还没有生成过日印象。'; return; }
        list.innerHTML = items.map((item, index) => {
            const summary = item.summary || '';
            const shortSummary = summary.length > 140 ? summary.slice(0, 140) + '...' : summary;
            const tags = _dailyTags(item);
            return '<div style="padding:10px 0;border-bottom:1px solid var(--border-color);cursor:pointer;" onclick="showDailyImpressionDetail(' + index + ')">' +
                '<div style="font-weight:600;margin-bottom:4px;">' + escHtml(item.date || '') + (item.mood ? ' · ' + escHtml(item.mood) : '') + '</div>' +
                '<div style="font-size:13px;line-height:1.5;color:var(--text-secondary);white-space:pre-wrap;">' + escHtml(shortSummary) + '</div>' +
                (tags ? '<div style="font-size:12px;color:var(--text-muted);margin-top:4px;">标签：' + escHtml(tags) + '</div>' : '') +
            '</div>';
        }).join('');
    } catch (e) {
        list.innerHTML = '<b>加载失败：</b>' + escHtml(e.message);
    }
}

function showDailyImpressionDetail(index) {
    const result = document.getElementById('dailyImpressionResult');
    if (result) result.style.display = 'block';
    _renderDailyDetail(_dailyImpressions[index], 'dailyImpressionResult');
}

async function loadDailyImpressionsPage() {
    const list = document.getElementById('dailyPageList');
    const detail = document.getElementById('dailyPageDetail');
    if (!list) return;
    list.innerHTML = '加载中...';
    if (detail) detail.style.display = 'none';
    try {
        const items = await _fetchDailyImpressions();
        if (!items.length) {
            list.innerHTML = '<div class="card" style="grid-column:1/-1;padding:28px;text-align:center;color:var(--text-muted);">还没有生成过日印象。</div>';
            return;
        }
        const palettes = [
            ['#fff1f2', '#e11d48'],
            ['#eef2ff', '#4f46e5'],
            ['#ecfeff', '#0891b2'],
            ['#f0fdf4', '#16a34a'],
            ['#fffbeb', '#d97706'],
            ['#fdf2f8', '#db2777'],
            ['#f5f3ff', '#7c3aed'],
            ['#f8fafc', '#475569']
        ];
        list.innerHTML = items.map((item, index) => {
            const tags = _dailyTags(item);
            const summary = item.summary || '';
            const shortSummary = summary.length > 72 ? summary.slice(0, 72) + '...' : summary;
            const p = palettes[index % palettes.length];
            return '<div onclick="showDailyPageDetail(' + index + ')" style="' +
                'min-height:170px;padding:16px;border-radius:18px;cursor:pointer;' +
                'background:' + p[0] + ';border:1px solid rgba(15,23,42,.06);' +
                'box-shadow:0 8px 24px rgba(15,23,42,.06);display:flex;flex-direction:column;gap:10px;' +
                '">' +
                '<div style="display:flex;justify-content:space-between;align-items:center;gap:8px;">' +
                    '<div style="font-weight:800;font-size:17px;color:' + p[1] + ';">' + escHtml(item.date || '') + '</div>' +
                    '<div style="font-size:18px;">📔</div>' +
                '</div>' +
                (item.mood ? '<div style="font-size:12px;color:rgba(15,23,42,.58);">' + escHtml(item.mood) + '</div>' : '') +
                '<div style="font-size:13px;line-height:1.55;color:rgba(15,23,42,.72);white-space:pre-wrap;flex:1;">' + escHtml(shortSummary) + '</div>' +
                (tags ? '<div style="font-size:12px;color:' + p[1] + ';font-weight:600;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;"># ' + escHtml(tags.replace(/、/g, ' # ')) + '</div>' : '') +
            '</div>';
        }).join('');
    } catch (e) {
        list.innerHTML = '<div class="card" style="grid-column:1/-1;padding:18px;color:#b91c1c;"><b>加载失败：</b>' + escHtml(e.message) + '</div>';
    }
}

function showDailyPageDetail(index) {
    const detail = document.getElementById('dailyPageDetail');
    if (!detail) return;
    _editingDailyIndex = -1;
    detail.style.display = 'block';
    const item = _dailyImpressions[index];
    const tags = _dailyTags(item);
    detail.innerHTML =
        '<div style="display:flex;justify-content:space-between;align-items:flex-start;margin-bottom:10px;">' +
            '<div style="font-size:13px;color:var(--text-muted);">' + escHtml(item.date || '') + (item.mood ? ' · ' + escHtml(item.mood) : '') + '</div>' +
            '<div style="display:flex;gap:8px;">' +
                '<button onclick="editDailyPageDetail(' + index + ')" style="padding:4px 10px;border:1px solid var(--border-color);border-radius:6px;background:#fff;cursor:pointer;font-size:12px;">编辑</button>' +
                '<button onclick="deleteDailyPageDetail(' + index + ')" style="padding:4px 10px;border:1px solid #dc2626;color:#dc2626;border-radius:6px;background:#fff;cursor:pointer;font-size:12px;">删除</button>' +
            '</div>' +
        '</div>' +
        '<div style="white-space:pre-wrap;line-height:1.7;font-size:15px;">' + escHtml(item.summary || '') + '</div>' +
        (tags ? '<div style="margin-top:12px;color:var(--text-muted);font-size:13px;">标签：' + escHtml(tags) + '</div>' : '');
}

function editDailyPageDetail(index) {
    _editingDailyIndex = index;
    const detail = document.getElementById('dailyPageDetail');
    if (!detail) return;
    const item = _dailyImpressions[index];
    const tags = _dailyTags(item);
    detail.innerHTML =
        '<div style="font-size:13px;color:var(--text-muted);margin-bottom:10px;">编辑日印象：' + escHtml(item.date || '') + '</div>' +
        '<div style="margin-bottom:10px;">' +
            '<label style="display:block;font-size:13px;margin-bottom:4px;">标签：</label>' +
            '<input id="editDailyTags" value="' + escHtml(tags) + '" style="width:100%;padding:8px;border:1px solid var(--border-color);border-radius:6px;" />' +
        '</div>' +
        '<div style="margin-bottom:10px;">' +
            '<label style="display:block;font-size:13px;margin-bottom:4px;">氛围：</label>' +
            '<input id="editDailyMood" value="' + escHtml(item.mood || '') + '" style="width:100%;padding:8px;border:1px solid var(--border-color);border-radius:6px;" />' +
        '</div>' +
        '<div style="margin-bottom:10px;">' +
            '<label style="display:block;font-size:13px;margin-bottom:4px;">正文：</label>' +
            '<textarea id="editDailySummary" rows="12" style="width:100%;padding:8px;border:1px solid var(--border-color);border-radius:6px;resize:vertical;">' + escHtml(item.summary || '') + '</textarea>' +
        '</div>' +
        '<div style="display:flex;gap:8px;">' +
            '<button onclick="saveDailyPageDetail()" style="padding:8px 16px;border:none;border-radius:6px;background:#4f46e5;color:#fff;cursor:pointer;">保存</button>' +
            '<button onclick="cancelDailyEdit()" style="padding:8px 16px;border:1px solid var(--border-color);border-radius:6px;background:#fff;cursor:pointer;">取消</button>' +
        '</div>';
}

async function saveDailyPageDetail() {
    if (_editingDailyIndex < 0) return;
    const item = _dailyImpressions[_editingDailyIndex];
    const summary = document.getElementById('editDailySummary')?.value?.trim() || '';
    const tags = document.getElementById('editDailyTags')?.value?.trim() || '';
    const mood = document.getElementById('editDailyMood')?.value?.trim() || '';
    if (!summary) { alert('正文不能为空'); return; }
    try {
        const resp = await fetch('/api/daily-impressions/' + (item.date || ''), {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({summary, tags, mood})
        });
        const data = await resp.json();
        if (data.error) throw new Error(data.error);
        await loadDailyImpressionsPage();
    } catch (e) {
        alert('保存失败：' + e.message);
    }
}

function cancelDailyEdit() {
    if (_editingDailyIndex >= 0) showDailyPageDetail(_editingDailyIndex);
}

async function deleteDailyPageDetail(index) {
    const item = _dailyImpressions[index];
    if (!confirm('确定删除 ' + (item.date || '') + ' 的日印象？')) return;
    try {
        const resp = await fetch('/api/daily-impressions/' + (item.date || ''), {method: 'DELETE'});
        const data = await resp.json();
        if (data.error) throw new Error(data.error);
        const detail = document.getElementById('dailyPageDetail');
        if (detail) detail.style.display = 'none';
        await loadDailyImpressionsPage();
    } catch (e) {
        alert('删除失败：' + e.message);
    }
}

function renderDailyRawBlock(raw) {
    if (!raw) return '';
    return '<details style="margin-top:10px;"><summary style="cursor:pointer;">查看模型返回原文</summary>' +
        '<pre style="white-space:pre-wrap;word-break:break-word;max-height:360px;overflow:auto;background:rgba(15,23,42,.05);border:1px solid var(--border-color);border-radius:8px;padding:10px;margin-top:8px;">' +
        escHtml(raw) + '</pre></details>';
}

async function generateDailyImpressionFromPage() {
    const dateInput = document.getElementById('dailyPageDate');
    const msg = document.getElementById('daily-page-msg');
    const date = dateInput ? dateInput.value : '';
    if (!date) { if (msg) msg.innerHTML = '<div class="msg msg-error">请选择日期</div>'; return; }
    if (msg) msg.innerHTML = '<div class="msg msg-info">正在生成日印象...</div>';
    try {
        const resp = await fetch('/api/daily-impressions/generate', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({date})
        });
        const data = await resp.json();
        if (data.error || data.status === 'error') throw new Error(data.error || '生成失败');
        if (data.status === 'no_conversations') {
            if (msg) msg.innerHTML = '<div class="msg msg-info">这一天没有对话历史</div>';
            return;
        }
        _lastDailyRaw = data.raw || '';
        if (msg) msg.innerHTML = '<div class="msg msg-success">✅ 已生成日印象（使用 ' + (data.messages_used || 0) + ' 条对话）</div>' + renderDailyRawBlock(_lastDailyRaw);
        await loadDailyImpressionsPage();
    } catch (e) {
        if (msg) msg.innerHTML = '<div class="msg msg-error">❌ ' + escHtml(e.message) + '</div>';
    }
}

(function patchDailyImpressionModal() {
    document.addEventListener('DOMContentLoaded', () => {
        const pageDate = document.getElementById('dailyPageDate');
        if (pageDate && !pageDate.value) pageDate.value = new Date().toISOString().slice(0, 10);
    });

    const originalOpen = window.openDailyImpressionModal;
    if (typeof originalOpen === 'function') {
        window.openDailyImpressionModal = function() {
            originalOpen();
            loadDailyImpressions();
        };
    }

    const originalGenerate = window.doGenerateDailyImpression;
    if (typeof originalGenerate === 'function') {
        window.doGenerateDailyImpression = async function() {
            await originalGenerate();
            loadDailyImpressions();
            loadDailyImpressionsPage();
        };
    }

    const originalSwitch = window.switchSection;
    if (typeof originalSwitch === 'function') {
        window.switchSection = function(name) {
            originalSwitch(name);
            if (name === 'daily-impressions') loadDailyImpressionsPage();
        };
    }
})();
