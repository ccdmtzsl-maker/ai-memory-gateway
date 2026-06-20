let _dailyImpressions = [];

async function loadDailyImpressions() {
    const list = document.getElementById('dailyImpressionList');
    if (!list) return;
    list.innerHTML = '加载中...';
    try {
        const resp = await fetch('/api/daily-impressions?limit=30');
        const data = await resp.json();
        if (data.error) {
            list.innerHTML = '<b>加载失败：</b>' + escHtml(data.error);
            return;
        }
        _dailyImpressions = data.impressions || [];
        if (!_dailyImpressions.length) {
            list.innerHTML = '还没有生成过日印象。';
            return;
        }
        list.innerHTML = _dailyImpressions.map((item, index) => {
            const summary = item.summary || '';
            const shortSummary = summary.length > 140 ? summary.slice(0, 140) + '...' : summary;
            return '<div style="padding:10px 0;border-bottom:1px solid var(--border-color);cursor:pointer;" onclick="showDailyImpressionDetail(' + index + ')">' +
                '<div style="font-weight:600;margin-bottom:4px;">' + escHtml(item.date || '') + (item.mood ? ' · ' + escHtml(item.mood) : '') + '</div>' +
                '<div style="font-size:13px;line-height:1.5;color:var(--text-secondary);white-space:pre-wrap;">' + escHtml(shortSummary) + '</div>' +
                (item.topics ? '<div style="font-size:12px;color:var(--text-muted);margin-top:4px;">主题：' + escHtml(item.topics) + '</div>' : '') +
            '</div>';
        }).join('');
    } catch (e) {
        list.innerHTML = '<b>加载失败：</b>' + escHtml(e.message);
    }
}

function showDailyImpressionDetail(index) {
    const item = _dailyImpressions[index];
    const result = document.getElementById('dailyImpressionResult');
    if (!item || !result) return;
    result.style.display = 'block';
    result.innerHTML =
        '<div style="font-size:13px;color:var(--text-muted);margin-bottom:6px;">' +
        escHtml(item.date || '') + (item.mood ? ' · ' + escHtml(item.mood) : '') + '</div>' +
        '<div style="white-space:pre-wrap;line-height:1.6;">' + escHtml(item.summary || '') + '</div>' +
        (item.topics ? '<div style="margin-top:8px;color:var(--text-muted);font-size:12px;">主题：' + escHtml(item.topics) + '</div>' : '');
}

(function patchDailyImpressionModal() {
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
        };
    }
})();
