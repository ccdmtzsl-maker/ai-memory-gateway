let _dailyImpressions = [];
let _lastDailyRaw = '';

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
    try {
        const items = await _fetchDailyImpressions();
        if (!items.length) {
            list.innerHTML = '还没有生成过日印象。';
            if (detail) detail.innerHTML = '<div style="color:var(--text-muted);text-align:center;padding:48px 0;">先选择日期生成一条日印象</div>';
            return;
        }
        list.innerHTML = items.map((item, index) => {
            const tags = _dailyTags(item);
            return '<div style="padding:12px;border-radius:8px;cursor:pointer;border:1px solid var(--border-color);margin-bottom:8px;" onclick="showDailyPageDetail(' + index + ')">' +
                '<div style="font-weight:700;">' + escHtml(item.date || '') + '</div>' +
                (item.mood ? '<div style="font-size:12px;color:var(--text-muted);margin-top:3px;">' + escHtml(item.mood) + '</div>' : '') +
                (tags ? '<div style="font-size:12px;color:var(--text-muted);margin-top:3px;">' + escHtml(tags) + '</div>' : '') +
            '</div>';
        }).join('');
        showDailyPageDetail(0);
    } catch (e) {
        list.innerHTML = '<b>加载失败：</b>' + escHtml(e.message);
    }
}

function showDailyPageDetail(index) {
    _renderDailyDetail(_dailyImpressions[index], 'dailyPageDetail');
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
