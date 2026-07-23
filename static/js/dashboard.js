/**
 * AI Memory Gateway - Dashboard JavaScript
 * 整合记忆宫殿、导入、导出、对话记录等功能
 */

// ============================================
// 内联 SVG 图标（Lucide 风格，24x24 viewBox）
// ============================================
const ICONS = (() => {
    const s = (inner, size = 16) => `<svg width="${size}" height="${size}" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round">${inner}</svg>`;
    return {
        brain:      (sz) => s('<path d="M9.5 2A2.5 2.5 0 0 1 12 4.5v15a2.5 2.5 0 0 1-4.96.44 2.5 2.5 0 0 1-2.96-3.08 3 3 0 0 1-.34-5.58 2.5 2.5 0 0 1 1.32-4.24 2.5 2.5 0 0 1 1.98-3A2.5 2.5 0 0 1 9.5 2Z"/><path d="M14.5 2A2.5 2.5 0 0 0 12 4.5v15a2.5 2.5 0 0 0 4.96.44 2.5 2.5 0 0 0 2.96-3.08 3 3 0 0 0 .34-5.58 2.5 2.5 0 0 0-1.32-4.24 2.5 2.5 0 0 0-1.98-3A2.5 2.5 0 0 0 14.5 2Z"/>', sz),
        download:   (sz) => s('<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" x2="12" y1="15" y2="3"/>', sz),
        upload:     (sz) => s('<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="17 8 12 3 7 8"/><line x1="12" x2="12" y1="3" y2="15"/>', sz),
        msgSquare:  (sz) => s('<path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>', sz),
        link:       (sz) => s('<path d="M10 13a5 5 0 0 0 7.54.54l3-3a5 5 0 0 0-7.07-7.07l-1.72 1.71"/><path d="M14 11a5 5 0 0 0-7.54-.54l-3 3a5 5 0 0 0 7.07 7.07l1.71-1.71"/>', sz),
        github:     (sz) => s('<path d="M15 22v-4a4.8 4.8 0 0 0-1-3.5c3 0 6-2 6-5.5.08-1.25-.27-2.48-1-3.5.28-1.15.28-2.35 0-3.5 0 0-1 0-3 1.5-2.64-.5-5.36-.5-8 0C6 2 5 2 5 2c-.3 1.15-.3 2.35 0 3.5A5.403 5.403 0 0 0 4 9c0 3.5 3 5.5 6 5.5-.39.49-.68 1.05-.85 1.65-.17.6-.22 1.23-.15 1.85v4"/><path d="M9 18c-4.51 2-5-2-7-2"/>', sz),
        search:     (sz) => s('<circle cx="11" cy="11" r="8"/><path d="m21 21-4.3-4.3"/>', sz),
        sparkles:   (sz) => s('<path d="m12 3-1.912 5.813a2 2 0 0 1-1.275 1.275L3 12l5.813 1.912a2 2 0 0 1 1.275 1.275L12 21l1.912-5.813a2 2 0 0 1 1.275-1.275L21 12l-5.813-1.912a2 2 0 0 1-1.275-1.275L12 3Z"/><path d="M5 3v4"/><path d="M19 17v4"/><path d="M3 5h4"/><path d="M17 19h4"/>', sz),
        x:          (sz) => s('<path d="M18 6 6 18"/><path d="m6 6 12 12"/>', sz),
        star:       (sz) => s('<polygon points="12 2 15.09 8.26 22 9.27 17 14.14 18.18 21.02 12 17.77 5.82 21.02 7 14.14 2 9.27 8.91 8.26 12 2"/>', sz),
        calendar:   (sz) => s('<rect width="18" height="18" x="3" y="4" rx="2"/><path d="M16 2v4"/><path d="M8 2v4"/><path d="M3 10h18"/>', sz),
        fileText:   (sz) => s('<path d="M14.5 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V7.5L14.5 2z"/><polyline points="14 2 14 8 20 8"/><line x1="16" x2="8" y1="13" y2="13"/><line x1="16" x2="8" y1="17" y2="17"/><line x1="10" x2="8" y1="9" y2="9"/>', sz),
        check:      (sz) => s('<polyline points="20 6 9 17 4 12"/>', sz),
        trash:      (sz) => s('<path d="M3 6h18"/><path d="M19 6v14c0 1-1 2-2 2H7c-1 0-2-1-2-2V6"/><path d="M8 6V4c0-1 1-2 2-2h4c1 0 2 1 2 2v2"/><line x1="10" x2="10" y1="11" y2="17"/><line x1="14" x2="14" y1="11" y2="17"/>', sz),
        rotateCcw:  (sz) => s('<path d="M3 12a9 9 0 1 0 9-9 9.75 9.75 0 0 0-6.74 2.74L3 8"/><path d="M3 3v5h5"/>', sz),
        undo:       (sz) => s('<path d="M9 14 4 9l5-5"/><path d="M4 9h10.5a5.5 5.5 0 0 1 5.5 5.5v0a5.5 5.5 0 0 1-5.5 5.5H11"/>', sz),
        paperclip:  (sz) => s('<path d="m21.44 11.05-9.19 9.19a6 6 0 0 1-8.49-8.49l8.57-8.57A4 4 0 1 1 18 8.84l-8.59 8.57a2 2 0 0 1-2.83-2.83l8.49-8.48"/>', sz),
        gitMerge:   (sz) => s('<circle cx="18" cy="18" r="3"/><circle cx="6" cy="6" r="3"/><path d="M6 21V9a9 9 0 0 0 9 9"/>', sz),
        calculator: (sz) => s('<rect width="16" height="20" x="4" y="2" rx="2"/><line x1="8" x2="16" y1="6" y2="6"/><line x1="16" x2="16" y1="14" y2="18"/><path d="M16 10h.01"/><path d="M12 10h.01"/><path d="M8 10h.01"/><path d="M12 14h.01"/><path d="M8 14h.01"/><path d="M12 18h.01"/><path d="M8 18h.01"/>', sz),
        save:       (sz) => s('<path d="M19 21H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11l5 5v11a2 2 0 0 1-2 2z"/><polyline points="17 21 17 13 7 13 7 21"/><polyline points="7 3 7 8 15 8"/>', sz),
    };
})();

// ============================================
// 网关鉴权：从URL参数读取gateway_key，自动注入所有请求
// ============================================
const _gatewayKey = new URLSearchParams(window.location.search).get('gateway_key') || '';
if (_gatewayKey) {
    const _origFetch = window.fetch;
    window.fetch = function(url, opts = {}) {
        opts.headers = opts.headers || {};
        if (opts.headers instanceof Headers) {
            opts.headers.set('X-Gateway-Key', _gatewayKey);
        } else {
            opts.headers['X-Gateway-Key'] = _gatewayKey;
        }
        return _origFetch.call(this, url, opts);
    };
}

// ============================================
// 全局状态
// ============================================
let pendingJsonData = null;

// ============================================
// 初始化
// ============================================
document.addEventListener('DOMContentLoaded', () => {
    // 初始化侧边栏导航
    initNavigation();
    // 初始化Tab切换
    initTabs();
    // 导出统计改为懒加载，只在切到导出页时才查
    // 默认打开后台日志页时，直接加载日志内容
    loadDashboardLogs();
});

// ============================================
// 侧边栏导航
// ============================================
function initNavigation() {
    const navItems = document.querySelectorAll('.nav-item[data-section]');
    navItems.forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            const section = item.dataset.section;
            switchSection(section);
        });
    });
}

function switchSection(name) {
    // 更新导航激活状态
    document.querySelectorAll('.nav-item[data-section]').forEach(item => {
        item.classList.toggle('active', item.dataset.section === name);
    });
    
    // 切换内容区域
    document.querySelectorAll('.section').forEach(section => {
        section.classList.toggle('active', section.id === 'section-' + name);
    });
    
    // 切换到导出页面时刷新统计
    if (name === 'export') {
        loadExportStats();
    }
    if (name === 'conversations') {
        loadConversationList(1);
        loadConvStats();
    }
    if (name === 'threads') {
        loadThreads();
    }
    if (name === 'user-impression') {
        loadUserImpression();
    }
    if (name === 'logs') {
        loadDashboardLogs();
    }
    if (name === 'settings') {
        loadSettings();
    }
}

// ============================================
// Tab 切换（导入页面）
// ============================================
function initTabs() {
    const tabs = document.querySelectorAll('.tab[data-tab]');
    tabs.forEach(tab => {
        tab.addEventListener('click', () => {
            const tabName = tab.dataset.tab;
            
            // 更新Tab激活状态
            document.querySelectorAll('.tab[data-tab]').forEach(t => {
                t.classList.toggle('active', t.dataset.tab === tabName);
            });
            
            // 切换Tab面板
            document.querySelectorAll('.tab-panel').forEach(panel => {
                panel.classList.toggle('active', panel.id === 'tab-' + tabName);
            });
            
            // 清除消息
            clearImportResult();
        });
    });
}

// ============================================
// 分层 Tab 切换
// ============================================
function escHtml(s) {
    if (!s) return '';
    return s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

function normalizeXmlToolMessageForExistingRenderer(msg) {
    const text = String((msg && msg.content) || '').trim();
    if (!text.startsWith('<tool')) return null;

    const callMatch = text.match(/^<tool\s+name="([^"]+)"\s*>([\s\S]*?)<\/tool>$/);
    if (callMatch) {
        const params = {};
        const body = callMatch[2] || '';
        const re = /<param\s+name="([^"]+)"\s*>([\s\S]*?)<\/param>/g;
        let m;
        while ((m = re.exec(body)) !== null) params[m[1]] = m[2] || '';
        const rowId = msg.id || msg.created_at || msg.timestamp || Math.random().toString(36).slice(2);
        return {
            role: 'assistant',
            content: '',
            metadata: {
                tool_calls: [{
                    id: 'xml_tool_' + String(rowId).replace(/[^\w-]/g, '_'),
                    type: 'function',
                    function: {
                        name: callMatch[1],
                        arguments: JSON.stringify(params, null, 2)
                    }
                }]
            }
        };
    }

    const resultMatch = text.match(/^<tool_result([\w-]*)\s+([^>]*)>([\s\S]*?)<\/tool_result[\w-]*>$/);
    if (resultMatch) {
        const attrs = {};
        const attrRe = /([A-Za-z_][\w-]*)="([^"]*)"/g;
        let a;
        while ((a = attrRe.exec(resultMatch[2] || '')) !== null) attrs[a[1]] = a[2];
        let body = resultMatch[3] || '';
        const contentMatch = body.match(/^<content>([\s\S]*?)<\/content>$/);
        if (contentMatch) body = contentMatch[1] || '';
        const suffix = (resultMatch[1] || '').replace(/^_/, '');
        return {
            role: 'tool',
            content: body,
            metadata: {
                name: attrs.name || '工具结果',
                status: attrs.status || '',
                tool_call_id: attrs.tool_call_id || attrs.id || suffix || ('xml_tool_result_' + String(msg.id || '').replace(/[^\w-]/g, '_'))
            }
        };
    }

    return null;
}


// ============================================
// 导入功能
// ============================================
async function previewJson() {
    const file = document.getElementById('jsonFile').files[0];
    const text = document.getElementById('jsonInput').value.trim();
    const preview = document.getElementById('jsonPreview');
    
    let jsonStr = '';
    if (file) {
        jsonStr = await file.text();
    } else if (text) {
        jsonStr = text;
    } else {
        showImportResult('error', '请先上传文件或粘贴 JSON');
        return;
    }
    
    try {
        const parsed = JSON.parse(jsonStr);
        const items = Array.isArray(parsed) ? parsed : (parsed.impressions || parsed.memories || []);
        if (items.length === 0) {
            showImportResult('error', '❌ 没有找到日印象数据，请确认这是从导出功能导出的文件');
            preview.innerHTML = '';
            return;
        }
        
        pendingJsonData = items;
        let html = '<p><b>预览：共 ' + items.length + ' 条日印象</b></p>';
        const show = items.slice(0, 10);
        show.forEach(m => {
            html += '<div class="preview-item">' + (m.date || '?') + ' | ' + (m.summary || '').substring(0, 80) + '</div>';
        });
        if (items.length > 10) {
            html += '<div class="preview-item" style="color:#999;">...还有 ' + (items.length - 10) + ' 条</div>';
        }
        html += '<br><button class="btn btn-primary" onclick="confirmJsonImport()">确认导入</button>';
        preview.innerHTML = html;
        clearImportResult();
    } catch(e) {
        showImportResult('error', '❌ JSON 格式错误：' + e.message);
        preview.innerHTML = '';
    }
}

async function confirmJsonImport() {
    if (!pendingJsonData) {
        showImportResult('error', '请先预览');
        return;
    }
    
    showImportResult('info', '导入中...');
    
    try {
        const resp = await fetch('/import/daily-impressions', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(pendingJsonData)
        });
        const data = await resp.json();
        if (data.error) {
            showImportResult('error', '❌ ' + data.error);
        } else {
            showImportResult('success', '✅ 导入完成！导入 ' + data.imported + ' 条，跳过 ' + data.skipped + ' 条');
        }
        document.getElementById('jsonPreview').innerHTML = '';
        pendingJsonData = null;
    } catch(e) {
        showImportResult('error', '❌ 请求失败：' + e.message);
    }
}

function showImportResult(type, text) {
    const container = document.getElementById('import-result');
    container.innerHTML = '<div class="msg msg-' + type + '">' + text + '</div>';
}

function clearImportResult() {
    document.getElementById('import-result').innerHTML = '';
    document.getElementById('jsonPreview').innerHTML = '';
}

// ============================================
// 导出功能
// ============================================
async function loadExportStats() {
    const el = document.getElementById('export-stats');
    const mpEl = document.getElementById('mp-export-stats');
    try {
        const resp = await fetch('/api/daily-impressions/stats');
        const data = await resp.json();
        if (data.error || data.status === 'error') throw new Error(data.error || '统计失败');
        const count = data.total || 0;
        if (el) el.textContent = '当前共有 ' + count + ' 条日印象';
    } catch(e) {
        if (el) el.textContent = '无法加载统计';
    }
    if (mpEl) {
        try {
            const resp = await fetch('/api/memory-palace/export-stats');
            const data = await resp.json();
            if (data.error) throw new Error(data.error);
            mpEl.textContent = '节点 ' + (data.total_nodes || 0) + ' 条 · 连接 ' + (data.total_links || 0) + ' 条 · 事件盒 ' + (data.total_event_boxes || 0) + ' 个';
        } catch(e) {
            mpEl.textContent = '无法加载统计';
        }
    }
}

async function exportDailyImpressions() {
    try {
        const resp = await fetch('/api/daily-impressions?limit=9999');
        const data = await resp.json();
        if (data.error) throw new Error(data.error);
        const impressions = data.impressions || [];
        if (!impressions.length) { alert('暂无日印象数据'); return; }
        const blob = new Blob([JSON.stringify(impressions, null, 2)], {type: 'application/json'});
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        const now = new Date();
        const ts = now.getFullYear() +
            String(now.getMonth() + 1).padStart(2, '0') +
            String(now.getDate()).padStart(2, '0') + '_' +
            String(now.getHours()).padStart(2, '0') +
            String(now.getMinutes()).padStart(2, '0') +
            String(now.getSeconds()).padStart(2, '0');
        a.href = url;
        a.download = 'daily_impressions_backup_' + ts + '.json';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    } catch (e) {
        alert('导出日印象失败: ' + e.message);
    }
}

async function exportMemoryPalace() {
    try {
        const resp = await fetch('/export/memory-palace');
        if (!resp.ok) throw new Error('HTTP ' + resp.status);
        const contentType = resp.headers.get('content-type') || '';
        if (contentType.includes('application/json')) {
            const clone = resp.clone();
            const data = await clone.json().catch(() => null);
            if (data && data.error) {
                alert('导出失败: ' + data.error);
                return;
            }
        }
        const blob = await resp.blob();
        const url = URL.createObjectURL(blob);
        const a = document.createElement('a');
        const now = new Date();
        const ts = now.getFullYear() +
            String(now.getMonth() + 1).padStart(2, '0') +
            String(now.getDate()).padStart(2, '0') + '_' +
            String(now.getHours()).padStart(2, '0') +
            String(now.getMinutes()).padStart(2, '0') +
            String(now.getSeconds()).padStart(2, '0');
        a.href = url;
        a.download = 'memory_palace_backup_' + ts + '.json';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    } catch (e) {
        alert('导出失败: ' + e.message);
    }
}


// ============================================
// 对话记录功能
// ============================================
let convCurrentPage = 1;
let convIsSearchMode = false;
let convSearchQuery = '';

async function loadConvStats() {
    const el = document.getElementById('conv-export-stats');
    if (!el) return;
    try {
        const resp = await fetch('/api/conversations?page=1&per_page=1');
        const data = await resp.json();
        el.textContent = '当前共有 ' + (data.total || 0) + ' 个对话';
    } catch(e) {
        el.textContent = '无法加载统计';
    }
}

async function exportConversations() {
    try {
        const resp = await fetch("/api/conversations/export");
        const data = await resp.json();
        if (data.error) { alert("导出失败: " + data.error); return; }
        const blob = new Blob([JSON.stringify(data, null, 2)], { type: "application/json" });
        const url = URL.createObjectURL(blob);
        const a = document.createElement("a");
        const now = new Date();
        const ts = now.getFullYear() +
            String(now.getMonth()+1).padStart(2,"0") +
            String(now.getDate()).padStart(2,"0") + "_" +
            String(now.getHours()).padStart(2,"0") +
            String(now.getMinutes()).padStart(2,"0") +
            String(now.getSeconds()).padStart(2,"0");
        a.href = url;
        a.download = "conversations_backup_" + ts + ".json";
        a.click();
        URL.revokeObjectURL(url);
    } catch(e) { alert("导出失败: " + e.message); }
}

async function doConvExport() { await exportConversations(); }

async function doConvImport() {
    const file = document.getElementById('convJsonFile').files[0];
    const text = document.getElementById('convJsonInput').value.trim();
    const resultEl = document.getElementById('conv-import-result');
    
    let jsonStr = '';
    if (file) { jsonStr = await file.text(); }
    else if (text) { jsonStr = text; }
    else { resultEl.innerHTML = '<div class="msg msg-error">请先上传文件或粘贴 JSON</div>'; return; }
    
    let records;
    try {
        records = JSON.parse(jsonStr);
        if (!Array.isArray(records)) records = records.records || records;
        if (!Array.isArray(records) || records.length === 0) {
            resultEl.innerHTML = '<div class="msg msg-error">❌ 没有找到有效的对话记录</div>';
            return;
        }
    } catch(e) {
        resultEl.innerHTML = '<div class="msg msg-error">❌ JSON 格式错误：' + e.message + '</div>';
        return;
    }
    
    if (!confirm('确定导入 ' + records.length + ' 条对话记录？')) return;
    
    // 分批导入（每批300条，避免超时）
    const BATCH_SIZE = 300;
    const totalBatches = Math.ceil(records.length / BATCH_SIZE);
    let totalImported = 0;
    let totalSkipped = 0;
    let failedBatches = 0;
    
    for (let i = 0; i < totalBatches; i++) {
        const batch = records.slice(i * BATCH_SIZE, (i + 1) * BATCH_SIZE);
        const progress = Math.round(((i + 1) / totalBatches) * 100);
        resultEl.innerHTML = `<div class="msg msg-info">导入中... 第 ${i + 1}/${totalBatches} 批（${progress}%）已导入 ${totalImported} 条</div>`;
        
        try {
            const resp = await fetch('/api/conversations/import', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify(batch)
            });
            const data = await resp.json();
            if (data.error) {
                failedBatches++;
                console.error(`批次 ${i + 1} 导入失败:`, data.error);
            } else {
                totalImported += (data.imported || 0);
                totalSkipped += (data.skipped || 0);
            }
        } catch(e) {
            failedBatches++;
            console.error(`批次 ${i + 1} 请求失败:`, e);
        }
    }
    
    let msg = `✅ 导入完成！新增 ${totalImported} 条`;
    if (totalSkipped) msg += `，跳过 ${totalSkipped} 条（已存在）`;
    if (failedBatches) msg += `，${failedBatches} 批失败`;
    resultEl.innerHTML = `<div class="msg msg-success">${msg}</div>`;
    
    loadConvStats();
    loadConversationList(1);
    document.getElementById('convJsonFile').value = '';
    document.getElementById('convJsonInput').value = '';
}


function setConvPlainMessage(container, text, opts = {}) {
    if (!container) return;
    container.textContent = '';
    const div = document.createElement('div');
    div.style.cssText = opts.style || 'text-align:center;color:var(--text-muted);padding:20px 0;';
    div.textContent = text || '';
    container.appendChild(div);
}

// 加载对话列表（分页）
async function loadConversationList(page = 1) {
    convCurrentPage = page;
    convIsSearchMode = false;
    convSearchQuery = '';
    document.getElementById('conv-search-input').value = '';
    document.getElementById('conv-search-status').textContent = '';
    document.getElementById('conv-list-title').textContent = '对话列表';
    
    const container = document.getElementById('conv-list-container');
    setConvPlainMessage(container, '加载中...');
    
    try {
        const resp = await fetch('/api/conversations?page=' + page + '&per_page=20');
        const data = await resp.json();
        if (data.error) {
            setConvPlainMessage(container, '加载失败: ' + data.error, {style:'color:var(--error);padding:20px 0;'});
            return;
        }
        renderConvList(data.conversations);
        renderConvPagination(data.page, data.total_pages, data.total);
        document.getElementById('conv-list-count').textContent = `共 ${data.total} 个对话`;
    } catch(e) {
        setConvPlainMessage(container, '请求失败: ' + e.message, {style:'color:var(--error);padding:20px 0;'});
    }
}

// 搜索对话
async function searchConversations() {
    const query = document.getElementById('conv-search-input').value.trim();
    if (!query) { loadConversationList(1); return; }
    
    convIsSearchMode = true;
    convSearchQuery = query;
    
    const container = document.getElementById('conv-list-container');
    const statusEl = document.getElementById('conv-search-status');
    setConvPlainMessage(container, '搜索中...');
    
    try {
        const resp = await fetch('/api/chat/search?q=' + encodeURIComponent(query) + '&limit=20&offset=0');
        if (resp.status === 404) { statusEl.textContent = '搜索功能暂未启用'; container.textContent = ''; return; }
        const data = await resp.json();
        if (data.error) {
            setConvPlainMessage(container, data.error, {style:'color:var(--error);padding:20px 0;'});
            return;
        }
        statusEl.textContent = `搜索"${query}"找到 ${data.total} 个对话`;
        document.getElementById('conv-list-title').textContent = '搜索结果';
        document.getElementById('conv-list-count').textContent = `${data.total} 个结果`;
        renderConvList(data.results, true);
        const pg = document.getElementById('conv-pagination');
        pg.textContent = '';
        if (data.total > 20) {
            const span = document.createElement('span');
            span.style.cssText = 'color:var(--text-muted);font-size:13px;';
            span.textContent = `显示前 20 条结果，共 ${data.total} 条`;
            pg.appendChild(span);
        }
    } catch(e) {
        setConvPlainMessage(container, '搜索失败: ' + e.message, {style:'color:var(--error);padding:20px 0;'});
    }
}

function clearConvSearch() {
    document.getElementById('conv-search-input').value = '';
    document.getElementById('conv-search-status').textContent = '';
    loadConversationList(1);
}

// 渲染对话列表
function renderConvList(conversations, isSearch = false) {
    const container = document.getElementById('conv-list-container');
    container.textContent = '';
    
    if (!conversations || conversations.length === 0) {
        const empty = document.createElement('div');
        empty.style.cssText = 'text-align:center;color:var(--text-muted);padding:40px 0;';
        empty.textContent = '暂无对话记录';
        container.appendChild(empty);
        return;
    }
    
    const bar = document.createElement('div');
    bar.id = 'conv-batch-bar';
    bar.style.cssText = 'display:flex;gap:8px;align-items:center;padding:8px 0;border-bottom:1px solid var(--border);margin-bottom:4px;';

    const label = document.createElement('label');
    label.style.cssText = 'display:flex;align-items:center;gap:4px;cursor:pointer;font-size:13px;';
    const selectAll = document.createElement('input');
    selectAll.type = 'checkbox';
    selectAll.id = 'conv-select-all';
    selectAll.addEventListener('change', () => toggleConvSelectAll(selectAll.checked));
    label.appendChild(selectAll);
    label.appendChild(document.createTextNode(' 全选'));
    bar.appendChild(label);

    const batchDeleteBtn = document.createElement('button');
    batchDeleteBtn.className = 'btn btn-sm';
    batchDeleteBtn.id = 'conv-batch-delete-btn';
    batchDeleteBtn.style.cssText = 'display:none;font-size:12px;';
    batchDeleteBtn.innerHTML = ICONS.trash(13) + ' 批量删除';
    batchDeleteBtn.addEventListener('click', batchDeleteConversations);
    bar.appendChild(batchDeleteBtn);

    const batchMergeBtn = document.createElement('button');
    batchMergeBtn.className = 'btn btn-sm';
    batchMergeBtn.id = 'conv-batch-merge-btn';
    batchMergeBtn.style.cssText = 'display:none;font-size:12px;';
    batchMergeBtn.innerHTML = ICONS.gitMerge(13) + ' 合并到...';
    batchMergeBtn.addEventListener('click', batchMergeSessions);
    bar.appendChild(batchMergeBtn);

    const mpBtn = document.createElement('button');
    mpBtn.className = 'btn btn-sm btn-primary';
    mpBtn.id = 'conv-batch-mp-btn';
    mpBtn.style.cssText = 'display:none;font-size:12px;';
    mpBtn.textContent = '🧠 提取记忆';
    mpBtn.addEventListener('click', previewMemoryPalaceFromSelectedConversations);
    bar.appendChild(mpBtn);

    const selectedCount = document.createElement('span');
    selectedCount.id = 'conv-selected-count';
    selectedCount.style.cssText = 'color:var(--text-muted);font-size:12px;display:none;';
    bar.appendChild(selectedCount);
    container.appendChild(bar);
    
    for (const conv of conversations) {
        container.appendChild(createConvListItem(conv));
    }
}

function createConvListItem(conv) {
    const sid = conv.session_id || conv.id || '';
    const title = sid;
    const preview = conv.title || conv.preview || '';
    const msgCount = conv.message_count || '';
    const totalTokens = conv.total_tokens || 0;
    const tokenStr = totalTokens > 0 ? (totalTokens >= 1000000 ? (totalTokens / 1000000).toFixed(1) + 'M' : totalTokens >= 1000 ? (totalTokens / 1000).toFixed(1) + 'K' : String(totalTokens)) : '';
    const lastTime = conv.last_time || conv.updated_at || '';
    const timeStr = lastTime ? formatConvTime(lastTime) : '';

    const item = document.createElement('div');
    item.className = 'conv-item';
    item.style.cssText = 'display:flex;align-items:flex-start;padding:12px;border-bottom:1px solid var(--border);transition:background 0.15s;';
    item.addEventListener('mouseover', () => { item.style.background = 'var(--bg-hover, rgba(0,0,0,0.03))'; });
    item.addEventListener('mouseout', () => { item.style.background = ''; });

    const cb = document.createElement('input');
    cb.type = 'checkbox';
    cb.className = 'conv-checkbox';
    cb.value = sid;
    cb.style.cssText = 'margin-right:10px;margin-top:4px;cursor:pointer;flex-shrink:0;';
    cb.addEventListener('change', updateConvSelectionCount);
    item.appendChild(cb);

    const clickable = document.createElement('div');
    clickable.style.cssText = 'flex:1;min-width:0;cursor:pointer;';
    clickable.addEventListener('click', () => openConvDetail(sid));

    const row = document.createElement('div');
    row.style.cssText = 'display:flex;justify-content:space-between;align-items:flex-start;';

    const left = document.createElement('div');
    left.style.cssText = 'flex:1;min-width:0;';
    const titleEl = document.createElement('div');
    titleEl.style.cssText = 'font-weight:500;margin-bottom:4px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;';
    titleEl.textContent = title;
    left.appendChild(titleEl);
    const previewEl = document.createElement('div');
    previewEl.style.cssText = 'color:var(--text-muted);font-size:13px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;';
    previewEl.textContent = preview;
    left.appendChild(previewEl);
    row.appendChild(left);

    const right = document.createElement('div');
    right.style.cssText = 'text-align:right;flex-shrink:0;margin-left:12px;';
    const timeEl = document.createElement('div');
    timeEl.style.cssText = 'color:var(--text-muted);font-size:12px;';
    timeEl.textContent = timeStr;
    right.appendChild(timeEl);
    if (msgCount) {
        const countEl = document.createElement('div');
        countEl.style.cssText = 'color:var(--text-muted);font-size:12px;margin-top:2px;';
        countEl.textContent = msgCount + ' 条';
        right.appendChild(countEl);
    }
    if (tokenStr) {
        const tokenEl = document.createElement('div');
        tokenEl.style.cssText = 'color:var(--text-muted);font-size:11px;margin-top:2px;';
        tokenEl.textContent = tokenStr;
        right.appendChild(tokenEl);
    }
    row.appendChild(right);
    clickable.appendChild(row);
    item.appendChild(clickable);

    return item;
}

// 渲染分页
function renderConvPagination(currentPage, totalPages, total) {
    const container = document.getElementById('conv-pagination');
    container.textContent = '';
    if (totalPages <= 1) return;
    
    const prev = document.createElement('button');
    prev.className = 'btn btn-sm';
    prev.textContent = '上一页';
    prev.disabled = currentPage <= 1;
    prev.addEventListener('click', () => loadConversationList(currentPage - 1));
    container.appendChild(prev);
    
    let startPage = Math.max(1, currentPage - 2);
    let endPage = Math.min(totalPages, startPage + 4);
    if (endPage - startPage < 4) startPage = Math.max(1, endPage - 4);
    
    for (let i = startPage; i <= endPage; i++) {
        const btn = document.createElement('button');
        btn.className = 'btn btn-sm' + (i === currentPage ? ' btn-primary' : '');
        btn.textContent = String(i);
        btn.addEventListener('click', () => loadConversationList(i));
        container.appendChild(btn);
    }
    
    const next = document.createElement('button');
    next.className = 'btn btn-sm';
    next.textContent = '下一页';
    next.disabled = currentPage >= totalPages;
    next.addEventListener('click', () => loadConversationList(currentPage + 1));
    container.appendChild(next);
    
    const info = document.createElement('span');
    info.style.cssText = 'color:var(--text-muted);font-size:12px;margin-left:8px;';
    info.textContent = `${currentPage}/${totalPages}`;
    container.appendChild(info);
}

// 打开对话详情
let convDetailSessionId = '';
let convDetailLoadedCount = 0;

async function openConvDetail(sessionId) {
    const panel = document.getElementById('conv-detail-panel');
    const titleEl = document.getElementById('conv-detail-title');
    const messagesEl = document.getElementById('conv-detail-messages');
    
    convDetailSessionId = sessionId;
    convDetailLoadedCount = 0;
    panel.style.display = 'block';
    titleEl.textContent = '加载中...';
    setConvPlainMessage(messagesEl, '加载中...');
    panel.scrollIntoView({ behavior: 'smooth', block: 'start' });
    
    await loadConvMessages(sessionId, false);
}

async function loadConvMessages(sessionId, append = false) {
    const titleEl = document.getElementById('conv-detail-title');
    const messagesEl = document.getElementById('conv-detail-messages');
    const offset = append ? convDetailLoadedCount : 0;
    
    try {
        const resp = await fetch(`/api/conversations/${encodeURIComponent(sessionId)}/messages?limit=30&offset=${offset}`);
        const data = await resp.json();
        
        if (data.error) {
            messagesEl.textContent = data.error;
            messagesEl.style.color = 'var(--error)';
            return;
        }
        messagesEl.style.color = '';
        
        const messages = data.messages || [];
        const total = data.total || messages.length;
        
        if (!append) {
            convDetailLoadedCount = 0;
            messagesEl.textContent = '';
            const bar = document.createElement('div');
            bar.style.cssText = 'margin-bottom:12px;display:flex;gap:8px;justify-content:flex-end;';
            const delBtn = document.createElement('button');
            delBtn.className = 'btn btn-sm';
            delBtn.innerHTML = ICONS.trash(13) + ' 删除对话';
            delBtn.addEventListener('click', () => deleteConversation(sessionId));
            bar.appendChild(delBtn);
            messagesEl.appendChild(bar);
        } else {
            const oldLoadMore = messagesEl.querySelector('.conv-load-more');
            if (oldLoadMore) oldLoadMore.remove();
        }
        convDetailLoadedCount += messages.length;
        
        titleEl.textContent = `对话详情（${convDetailLoadedCount} / ${total} 条消息）`;
        
        for (const msg of messages) {
            messagesEl.appendChild(createConvMessageElement(msg));
        }
        
        if (convDetailLoadedCount < total) {
            const moreWrap = document.createElement('div');
            moreWrap.className = 'conv-load-more';
            moreWrap.style.cssText = 'text-align:center;padding:16px 0;';
            const moreBtn = document.createElement('button');
            moreBtn.className = 'btn btn-primary';
            moreBtn.textContent = `加载更多（还有 ${total - convDetailLoadedCount} 条）`;
            moreBtn.addEventListener('click', () => loadConvMessages(sessionId, true));
            moreWrap.appendChild(moreBtn);
            messagesEl.appendChild(moreWrap);
        }
    } catch(e) {
        if (!append) {
            messagesEl.textContent = '加载失败: ' + e.message;
            messagesEl.style.color = 'var(--error)';
        }
    }
}

function createConvMessageElement(msg) {
    const xmlNormalized = normalizeXmlToolMessageForExistingRenderer(msg);
    if (xmlNormalized) msg = Object.assign({}, msg, xmlNormalized);
    const isUser = msg.role === 'user';
    const isTool = msg.role === 'tool';
    const roleLabel = isUser ? '👤 用户' : (isTool ? '🧰 工具结果' : '🤖 助手');
    const bgColor = isUser ? 'var(--bg-user, rgba(59,130,246,0.08))' : (isTool ? 'rgba(245,158,11,0.08)' : 'var(--bg-assistant, rgba(0,0,0,0.02))');
    const timeStr = msg.created_at ? formatConvTime(msg.created_at) : '';
    const msgId = msg.id || '';
    const meta = Object.assign({}, msg.metadata || {});
    if (msg.tool_call_id && !meta.tool_call_id) meta.tool_call_id = msg.tool_call_id;
    if (msg.name && !meta.name) meta.name = msg.name;
    if (msg.tool_calls && !meta.tool_calls) meta.tool_calls = msg.tool_calls;
    let displayContent = msg.content || '';
    if (!displayContent && meta.tool_calls && Array.isArray(meta.tool_calls)) {
        displayContent = ' ';
    } else if (isTool && meta.tool_call_id) {
        displayContent = `tool_call_id: ${meta.tool_call_id}\n\n${displayContent}`;
    }

    const wrap = document.createElement('div');
    wrap.id = msgId ? `msg-${msgId}` : '';
    wrap.style.cssText = `padding:12px;margin-bottom:8px;border-radius:8px;background:${bgColor};position:relative;`;

    const header = document.createElement('div');
    header.style.cssText = 'display:flex;justify-content:space-between;align-items:center;margin-bottom:6px;';

    const role = document.createElement('span');
    role.style.cssText = 'font-weight:500;font-size:13px;';
    role.textContent = roleLabel;
    header.appendChild(role);

    const tools = document.createElement('div');
    tools.style.cssText = 'display:flex;align-items:center;gap:8px;';
    const time = document.createElement('span');
    time.style.cssText = 'color:var(--text-muted);font-size:12px;';
    time.textContent = timeStr;
    tools.appendChild(time);

    if (msgId) {
        const editBtn = document.createElement('button');
        editBtn.className = 'btn btn-sm';
        editBtn.style.cssText = 'font-size:11px;padding:2px 8px;';
        editBtn.textContent = '编辑';
        editBtn.addEventListener('click', () => toggleEditMessage(msgId));
        tools.appendChild(editBtn);

        const delBtn = document.createElement('button');
        delBtn.className = 'btn btn-sm';
        delBtn.style.cssText = 'font-size:11px;padding:2px 8px;color:var(--error);';
        delBtn.textContent = '删除';
        delBtn.addEventListener('click', () => deleteSingleMessage(msgId));
        tools.appendChild(delBtn);
    }
    header.appendChild(tools);
    wrap.appendChild(header);

    const content = document.createElement('div');
    content.className = 'msg-content';
    content.id = msgId ? `msg-content-${msgId}` : '';
    content.style.cssText = 'white-space:pre-wrap;word-break:break-word;font-size:14px;line-height:1.6;';
    content.textContent = displayContent;
    wrap.appendChild(content);

    const edit = document.createElement('div');
    edit.className = 'msg-edit';
    edit.id = msgId ? `msg-edit-${msgId}` : '';
    edit.style.display = 'none';

    const textarea = document.createElement('textarea');
    textarea.id = msgId ? `msg-textarea-${msgId}` : '';
    textarea.style.cssText = 'width:100%;min-height:180px;padding:10px 12px;border:1px solid var(--border);border-radius:8px;font-size:14px;line-height:1.7;resize:vertical;font-family:inherit;box-sizing:border-box;';
    textarea.value = displayContent;
    edit.appendChild(textarea);

    const editActions = document.createElement('div');
    editActions.style.cssText = 'margin-top:8px;display:flex;gap:8px;justify-content:flex-end;';
    const cancelBtn = document.createElement('button');
    cancelBtn.className = 'btn btn-sm';
    cancelBtn.textContent = '取消';
    cancelBtn.addEventListener('click', () => toggleEditMessage(msgId));
    editActions.appendChild(cancelBtn);
    const saveBtn = document.createElement('button');
    saveBtn.className = 'btn btn-sm btn-primary';
    saveBtn.textContent = '保存';
    saveBtn.addEventListener('click', () => saveMessageEdit(msgId));
    editActions.appendChild(saveBtn);
    edit.appendChild(editActions);
    wrap.appendChild(edit);

    return wrap;
}

function setConvDetailEditScrollLock(locked) {
    const container = document.getElementById('conv-detail-messages');
    if (!container) return;
    if (locked) {
        if (container.dataset.prevOverflowY === undefined) {
            container.dataset.prevOverflowY = container.style.overflowY || '';
        }
        container.style.overflowY = 'hidden';
    } else {
        if (container.dataset.prevOverflowY !== undefined) {
            container.style.overflowY = container.dataset.prevOverflowY;
            delete container.dataset.prevOverflowY;
        } else {
            container.style.overflowY = 'auto';
        }
    }
}

function closeConvDetail() {
    setConvDetailEditScrollLock(false);
    document.getElementById('conv-detail-panel').style.display = 'none';
}

// 编辑消息
function toggleEditMessage(msgId) {
    const contentEl = document.getElementById('msg-content-' + msgId);
    const editEl = document.getElementById('msg-edit-' + msgId);
    const opening = editEl.style.display === 'none';
    
    if (opening) {
        contentEl.style.display = 'none';
        editEl.style.display = 'block';
        setConvDetailEditScrollLock(true);
        const textarea = document.getElementById('msg-textarea-' + msgId);
        if (textarea) textarea.focus();
    } else {
        contentEl.style.display = '';
        editEl.style.display = 'none';
        setConvDetailEditScrollLock(false);
    }
}

async function saveMessageEdit(msgId) {
    const textarea = document.getElementById('msg-textarea-' + msgId);
    const newContent = textarea.value.trim();
    if (!newContent) { alert('内容不能为空'); return; }
    
    try {
        const resp = await fetch(`/api/chat/messages/${msgId}`, {
            method: 'PATCH',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ content: newContent })
        });
        if (resp.status === 404) { alert('消息编辑功能暂未启用'); return; }
        const data = await resp.json();
        if (data.error) {
            alert('保存失败: ' + data.error);
            return;
        }
        
        // 更新显示
        const contentEl = document.getElementById('msg-content-' + msgId);
        contentEl.textContent = newContent;
        toggleEditMessage(msgId);
    } catch(e) {
        alert('请求失败: ' + e.message);
    }
}

// 删除单条消息
async function deleteSingleMessage(msgId) {
    if (!confirm('确定删除这条消息？此操作不可撤销。')) return;
    try {
        const resp = await fetch('/api/messages/' + msgId, { method: 'DELETE' });
        const data = await resp.json();
        if (data.error) {
            alert('删除失败: ' + data.error);
            return;
        }
        const msgEl = document.getElementById('msg-' + msgId);
        if (msgEl) msgEl.remove();
        setConvDetailEditScrollLock(false);
        const titleEl = document.getElementById('conv-detail-title');
        if (titleEl) {
            const m = titleEl.textContent.match(/(\d+)\s*\/\s*(\d+)/);
            if (m) {
                const loaded = parseInt(m[1]) - 1;
                const total = parseInt(m[2]) - 1;
                titleEl.textContent = `对话详情（${loaded} / ${total} 条消息）`;
            }
        }
    } catch(e) {
        alert('请求失败: ' + e.message);
    }
}

// 删除对话
async function deleteConversation(sessionId) {
    if (!confirm('确定删除这个对话吗？（可在回收站恢复）')) return;
    
    try {
        const resp = await fetch(`/api/conversations/${encodeURIComponent(sessionId)}`, { method: 'DELETE' });
        const data = await resp.json();
        if (data.error) {
            alert('删除失败: ' + data.error);
            return;
        }
        closeConvDetail();
        if (convIsSearchMode) {
            searchConversations();
        } else {
            loadConversationList(convCurrentPage);
        }
    } catch(e) {
        alert('请求失败: ' + e.message);
    }
}

// 多选功能
function toggleConvSelectAll(checked) {
    document.querySelectorAll('.conv-checkbox').forEach(cb => { cb.checked = checked; });
    updateConvSelectionCount();
}

function updateConvSelectionCount() {
    const checked = document.querySelectorAll('.conv-checkbox:checked');
    const countEl = document.getElementById('conv-selected-count');
    const btnEl = document.getElementById('conv-batch-delete-btn');
    const mergeBtn = document.getElementById('conv-batch-merge-btn');
    const mpBtn = document.getElementById('conv-batch-mp-btn');
    const allCb = document.getElementById('conv-select-all');
    const allCheckboxes = document.querySelectorAll('.conv-checkbox');
    
    
    if (checked.length > 0) {
        countEl.style.display = '';
        countEl.textContent = `已选 ${checked.length} 个`;
        btnEl.style.display = '';
        if (mergeBtn) mergeBtn.style.display = '';
        if (mpBtn) mpBtn.style.display = '';
    } else {
        countEl.style.display = 'none';
        btnEl.style.display = 'none';
        if (mergeBtn) mergeBtn.style.display = 'none';
        if (mpBtn) mpBtn.style.display = 'none';
    }
    
    if (allCb) {
        allCb.checked = allCheckboxes.length > 0 && checked.length === allCheckboxes.length;
    }
}

async function batchDeleteConversations() {
    const checked = document.querySelectorAll('.conv-checkbox:checked');
    if (checked.length === 0) return;
    
    if (!confirm(`确定删除选中的 ${checked.length} 个对话吗？（可在回收站恢复）`)) return;
    
    const sessionIds = Array.from(checked).map(cb => cb.value);
    
    try {
        const resp = await fetch('/api/conversations/batch-delete', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ session_ids: sessionIds })
        });
        const data = await resp.json();
        if (data.error) {
            alert('批量删除失败: ' + data.error);
            return;
        }
        
        if (convIsSearchMode) {
            searchConversations();
        } else {
            loadConversationList(convCurrentPage);
        }
    } catch(e) {
        alert('请求失败: ' + e.message);
    }
}

async function batchMergeSessions() {
    const checked = document.querySelectorAll('.conv-checkbox:checked');
    if (checked.length === 0) return;
    
    const targetId = prompt('输入目标 Session ID（所有选中的对话将合并到这个session）:', 'interlocked');
    if (!targetId) return;
    
    const sessionIds = Array.from(checked).map(cb => cb.value);
    
    if (!confirm(`确定将选中的 ${sessionIds.length} 个对话合并到「${targetId}」吗？\n\n此操作不可撤销。`)) return;
    
    try {
        const resp = await fetch('/api/admin/merge-sessions', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ source_ids: sessionIds, target_id: targetId })
        });
        const data = await resp.json();
        if (data.error) {
            alert('合并失败: ' + data.error);
            return;
        }
        
        alert(`合并完成！\n${data.merged_sessions} 个session → ${targetId}\n${data.merged_messages} 条消息\n${data.merged_token_records} 条token记录`);
        loadConversationList(convCurrentPage);
    } catch(e) {
        alert('请求失败: ' + e.message);
    }
}



let _convMemoryPalacePreviewItems = [];
let _convMemoryPalacePreviewRunning = false;
function convMpPanel(){let p=document.getElementById('conv-memory-preview-panel');if(!p){const c=document.getElementById('conv-list-container');p=document.createElement('div');p.id='conv-memory-preview-panel';p.className='card';p.style.marginTop='12px';p.style.display='none';c.parentNode.insertBefore(p,c);}return p;}
async function previewMemoryPalaceFromSelectedConversations() {
    if (_convMemoryPalacePreviewRunning) return;
    const checked = document.querySelectorAll('.conv-checkbox:checked');
    if (!checked.length) return;
    _convMemoryPalacePreviewRunning = true;
    const btn = document.getElementById('conv-batch-mp-btn');
    const oldText = btn ? btn.textContent : '';
    if (btn) { btn.disabled = true; btn.textContent = '🧠 提取中...'; }
    const sessionIds = Array.from(checked).map(cb => cb.value);
    const p = convMpPanel();
    p.style.display = '';
    p.textContent = '';
    const loading = document.createElement('div');
    loading.style.cssText = 'padding:14px;color:var(--text-muted);line-height:1.7;';
    loading.textContent = '🧠 正在逐个对话线提取记忆预览... 已发送请求，模型提取可能需要几十秒。你可以稍等一下。';
    p.appendChild(loading);
    try { p.scrollIntoView({behavior:'smooth',block:'start'}); } catch(_e) {}
    try {
        const r = await fetch('/api/memory-palace/extract-preview-sessions', {
            method:'POST', headers:{'Content-Type':'application/json'},
            body:JSON.stringify({session_ids:sessionIds, character_id:'default'})
        });
        const d = await r.json();
        if (d.error || d.status === 'error') {
            setConvPlainMessage(p, '提取失败：' + (d.error || '未知错误'), {style:'color:var(--error);padding:12px;'});
            return;
        }
        renderConvMemoryPalacePreview(d.groups || []);
    } catch(e) {
        setConvPlainMessage(p, '请求失败：' + e.message, {style:'color:var(--error);padding:12px;'});
    } finally {
        _convMemoryPalacePreviewRunning = false;
        if (btn) { btn.disabled = false; btn.textContent = oldText || '🧠 提取记忆'; }
    }
}
function renderConvMemoryPalacePreview(groups) {
    const p = convMpPanel();
    p.textContent = '';
    _convMemoryPalacePreviewItems = [];

    const header = document.createElement('div');
    header.style.cssText = 'display:flex;justify-content:space-between;align-items:center;margin-bottom:12px;';
    const headLeft = document.createElement('div');
    const h4 = document.createElement('h4');
    h4.style.margin = '0';
    h4.textContent = '记忆宫殿提取预览';
    headLeft.appendChild(h4);
    const sub = document.createElement('div');
    sub.style.cssText = 'font-size:12px;color:var(--text-muted);';
    sub.textContent = '逐个对话线处理；勾选后才会真正导入。';
    headLeft.appendChild(sub);
    header.appendChild(headLeft);
    const closeBtn = document.createElement('button');
    closeBtn.className = 'btn btn-sm';
    closeBtn.textContent = '关闭';
    closeBtn.addEventListener('click', closeConvMemoryPreview);
    header.appendChild(closeBtn);
    p.appendChild(header);

    let idx = 0;
    for (const g of groups) {
        const groupEl = document.createElement('div');
        groupEl.style.cssText = 'border-top:1px solid var(--border);padding-top:10px;margin-top:10px;';
        const title = document.createElement('b');
        title.textContent = '【对话线：' + (g.session_id || '') + '】';
        groupEl.appendChild(title);
        groupEl.appendChild(document.createTextNode(' '));

        if (g.status !== 'ok') {
            const err = document.createElement('span');
            err.style.cssText = 'color:var(--error);font-size:13px;';
            err.textContent = g.error || g.message || '没有结果';
            groupEl.appendChild(err);
            p.appendChild(groupEl);
            continue;
        }

        const stat = document.createElement('span');
        stat.style.cssText = 'font-size:12px;color:var(--text-muted);';
        stat.textContent = `${g.message_count || 0} 条消息，${g.memory_count || 0} 条记忆`;
        groupEl.appendChild(stat);

        const items = g.items || [];
        if (!items.length) {
            const none = document.createElement('div');
            none.style.cssText = 'color:var(--text-muted);font-size:13px;margin-top:8px;';
            none.textContent = '没有提取出候选记忆。';
            groupEl.appendChild(none);
            p.appendChild(groupEl);
            continue;
        }

        for (const item of items) {
            const cur = idx++;
            _convMemoryPalacePreviewItems.push(item);
            const label = document.createElement('label');
            label.style.cssText = 'display:block;margin:8px 0;padding:10px;border:1px solid var(--border);border-radius:8px;';
            if (item.type === 'unpin') label.style.background = 'rgba(250,204,21,.08)';
            const cb = document.createElement('input');
            cb.type = 'checkbox';
            cb.className = 'conv-mp-preview-check';
            cb.value = String(cur);
            cb.checked = true;
            label.appendChild(cb);
            label.appendChild(document.createTextNode(' '));

            if (item.type === 'unpin') {
                const b = document.createElement('b');
                b.textContent = '📌 摘除便利贴';
                label.appendChild(b);
                const content = document.createElement('div');
                content.style.cssText = 'margin-top:6px;font-size:13px;';
                content.textContent = item.content || '';
                label.appendChild(content);
            } else {
                let meta = '房间:' + (item.room || '') + '｜重要性:' + (item.importance || 5) + '｜情绪:' + (item.mood || 'neutral') + '｜日期:' + (item.date || '');
                if (item.pinned_until) meta += '｜📌便利贴';
                const metaEl = document.createElement('span');
                metaEl.style.cssText = 'color:var(--text-muted);font-size:12px;';
                metaEl.textContent = meta;
                label.appendChild(metaEl);
                const content = document.createElement('div');
                content.style.cssText = 'margin-top:6px;white-space:pre-wrap;';
                content.textContent = item.content || '';
                label.appendChild(content);
            }
            groupEl.appendChild(label);
        }
        p.appendChild(groupEl);
    }

    const actions = document.createElement('div');
    actions.style.cssText = 'display:flex;gap:8px;justify-content:flex-end;margin-top:12px;';
    const noneBtn = document.createElement('button');
    noneBtn.className = 'btn btn-sm';
    noneBtn.textContent = '全不选';
    noneBtn.addEventListener('click', () => toggleConvMemoryPreviewChecks(false));
    actions.appendChild(noneBtn);
    const allBtn = document.createElement('button');
    allBtn.className = 'btn btn-sm';
    allBtn.textContent = '全选';
    allBtn.addEventListener('click', () => toggleConvMemoryPreviewChecks(true));
    actions.appendChild(allBtn);
    const importBtn = document.createElement('button');
    importBtn.className = 'btn btn-primary btn-sm';
    importBtn.textContent = '导入选中';
    importBtn.addEventListener('click', importSelectedConvMemoryPreview);
    actions.appendChild(importBtn);
    p.appendChild(actions);
}
function toggleConvMemoryPreviewChecks(v){document.querySelectorAll('.conv-mp-preview-check').forEach(cb=>cb.checked=v);}
function closeConvMemoryPreview(){const p=document.getElementById('conv-memory-preview-panel');if(p)p.style.display='none';_convMemoryPalacePreviewItems=[];}

async function importSelectedConvMemoryPreview() {
    const p = convMpPanel();
    const checks = Array.from(document.querySelectorAll('.conv-mp-preview-check:checked'));
    const items = checks
        .map(cb => _convMemoryPalacePreviewItems[parseInt(cb.value, 10)])
        .filter(Boolean);

    if (!items.length) {
        setConvPlainMessage(p, '请先勾选要导入的记忆。', {style:'color:var(--error);padding:12px;'});
        return;
    }

    const btn = Array.from(p.querySelectorAll('button')).find(b => (b.textContent || '').includes('导入选中'));
    const oldText = btn ? btn.textContent : '';
    if (btn) { btn.disabled = true; btn.textContent = '导入中...'; }

    try {
        const r = await fetch('/api/memory-palace/import-preview', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({items, character_id:'default'})
        });
        const d = await r.json();
        if (!r.ok || d.error || d.status === 'error') {
            throw new Error(d.error || ('HTTP ' + r.status));
        }
        const msg = '导入完成：新增' + (d.created || 0) + '条，embedding ' + (d.embedded || 0) +
            '条，事件盒' + (d.event_boxes || 0) + '个，纠错' + (d.corrected || 0) +
            '条，摘除便利贴' + (d.unpinned || 0) + '条，标记' + (d.marked || 0) + '条。';
        setConvPlainMessage(p, msg, {style:'color:var(--success);padding:12px;'});
        _convMemoryPalacePreviewItems = [];
        try { loadConversationList(convCurrentPage); } catch(_e) {}
        try { loadMemoryPalace(); } catch(_e) {}
    } catch(e) {
        setConvPlainMessage(p, '导入失败：' + e.message, {style:'color:var(--error);padding:12px;'});
    } finally {
        if (btn) { btn.disabled = false; btn.textContent = oldText || '导入选中'; }
    }
}

// 工具函数
function escapeHtml(str) {
    const div = document.createElement('div');
    div.textContent = str;
    return div.innerHTML;
}

async function loadDashboardLogs() {
    const list = document.getElementById('dashboard-log-list');
    if (!list) return;
    list.innerHTML = '<div class="card" style="padding:24px; text-align:center; color:var(--text-muted);">加载日志中...</div>';
    try {
        const resp = await fetch('/api/dashboard/logs?limit=120');
        const data = await resp.json();
        const logs = data.logs || [];
        if (!logs.length) {
            list.innerHTML = '<div class="card" style="padding:30px; text-align:center; color:var(--text-muted);">🫧 暂时没有后台日志。发几条消息后再刷新看看。</div>';
            return;
        }
        const colorMap = {
            success: ['#16a34a', 'rgba(22,163,74,.10)'],
            run: ['#7c3aed', 'rgba(124,58,237,.10)'],
            skip: ['#64748b', 'rgba(100,116,139,.10)'],
            empty: ['#0891b2', 'rgba(8,145,178,.10)'],
            warn: ['#d97706', 'rgba(217,119,6,.12)'],
            error: ['#dc2626', 'rgba(220,38,38,.10)']
        };
        list.innerHTML = logs.map(log => {
            const colors = colorMap[log.level] || ['var(--primary)', 'rgba(231,90,124,.08)'];
            const sid = log.session_id ? `<span style="font-size:12px; color:var(--text-muted);">session: ${escapeHtml(log.session_id)}</span>` : '';
            const message = String(log.message || '');
            const isMultiLine = message.includes('\n');
            const isToolChain = message.startsWith('🔧 tool_chain[') || message.startsWith('🔧 tool_call_id');
            const msgHtml = (isMultiLine || isToolChain)
                ? `<pre style="margin:0; padding:10px 12px; border-radius:8px; background:rgba(255,255,255,.55); color:var(--text); white-space:pre-wrap; word-break:break-word; overflow:auto; font-size:12px; line-height:1.55; font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,'Liberation Mono',monospace;">${escapeHtml(message)}</pre>`
                : `<div style="font-size:14px; line-height:1.6; color:var(--text); word-break:break-word;">${escapeHtml(message)}</div>`;
            return `<div class="card" style="padding:14px 16px; border-left:4px solid ${colors[0]}; background:${colors[1]};">
                <div style="display:flex; justify-content:space-between; gap:12px; align-items:flex-start; margin-bottom:6px;">
                    <div style="display:flex; gap:8px; align-items:center; flex-wrap:wrap;">
                        <span style="font-weight:700; color:${colors[0]}; font-size:13px;">${escapeHtml(log.level || 'log')}</span>
                        ${sid}
                    </div>
                    <span style="font-size:12px; color:var(--text-muted); white-space:nowrap;">${escapeHtml(log.time || '')}</span>
                </div>
                ${msgHtml}
            </div>`;
        }).join('');
    } catch (e) {
        list.innerHTML = '<div class="card" style="padding:20px; color:#dc2626;">加载日志失败：' + escapeHtml(e.message || String(e)) + '</div>';
    }
}

async function clearDashboardLogs() {
    if (!confirm('确定清空当前后台日志？')) return;
    await fetch('/api/dashboard/logs/clear', {method: 'POST'});
    loadDashboardLogs();
}

async function toggleLastRequestBody(forceShow) {
    const panel = document.getElementById('last-request-body-panel');
    const content = document.getElementById('last-request-body-content');
    const metaEl = document.getElementById('last-request-body-meta');
    if (!panel || !content) return;

    const shouldShow = forceShow === undefined ? panel.style.display === 'none' : forceShow;
    if (!shouldShow) {
        panel.style.display = 'none';
        return;
    }

    panel.style.display = 'block';
    content.textContent = '加载上次请求体中...';
    if (metaEl) metaEl.textContent = '';

    try {
        const resp = await fetch('/api/dashboard/last-request');
        const data = await resp.json();
        if (!data.available) {
            content.textContent = data.message || '还没有记录到已转发的请求体';
            return;
        }

        const meta = data.meta || {};
        if (metaEl) {
            const parts = [];
            if (meta.time) parts.push(meta.time);
            if (meta.model) parts.push('model: ' + meta.model);
            if (meta.session_id) parts.push('session: ' + meta.session_id);
            if (meta.message_count !== undefined) parts.push('messages: ' + meta.message_count);
            parts.push('分区缓存: ' + (meta.cache_partition_enabled ? '开' : '关'));
            metaEl.textContent = parts.join(' · ');
        }
        content.textContent = JSON.stringify(data.body || {}, null, 2);
    } catch (e) {
        content.textContent = '读取上次请求体失败：' + (e.message || String(e));
    }
}

function formatConvTime(isoStr) {
    try {
        const d = new Date(isoStr);
        const now = new Date();
        const diffMs = now - d;
        const diffDays = Math.floor(diffMs / 86400000);
        
        if (diffDays === 0) {
            return d.toLocaleTimeString('zh-CN', { hour: '2-digit', minute: '2-digit' });
        } else if (diffDays === 1) {
            return '昨天';
        } else if (diffDays < 7) {
            return diffDays + '天前';
        } else {
            return d.toLocaleDateString('zh-CN', { month: 'short', day: 'numeric' });
        }
    } catch(e) {
        return '';
    }
}

// ============================================
// 对话线管理
// ============================================

let _threadData = { threads: [], active_session_id: '' };
let _summaryEditSid = '';

async function loadThreads() {
    try {
        const [statusResp, threadsResp] = await Promise.all([
            fetch('/api/partition/status'),
            fetch('/api/partition/threads')
        ]);
        const status = await statusResp.json();
        const data = await threadsResp.json();
        _threadData = data;
        
        renderThreadStatus(status);
        renderThreadList(data.threads);
    } catch(e) {
        document.getElementById('thread-status').textContent = '加载失败: ' + e.message;
    }
}

function renderThreadStatus(status) {
    const el = document.getElementById('thread-status');
    if (!status.enabled) {
        el.innerHTML = '<span style="color: var(--danger);">⚠️ 分区缓存未启用（CACHE_PARTITION_ENABLED=false）</span>';
        return;
    }
    
    el.innerHTML = `
        <div style="display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px;">
            <div><strong>活跃对话线</strong><br><span style="font-size: 18px; color: var(--primary);">${status.active_session_id || '未设置'}</span></div>
            <div><strong>轮转周期</strong><br>每 ${status.partition_x} 轮</div>
            <div><strong>A区起始轮</strong><br>第 ${status.a_start_round} 轮</div>
        </div>
    `;
}

function renderThreadList(threads) {
    const el = document.getElementById('thread-list');
    if (!threads || threads.length === 0) {
        el.innerHTML = '<div style="text-align: center; color: var(--text-muted); padding: 20px 0;">暂无对话线</div>';
        return;
    }
    
    let html = '';
    for (const t of threads) {
        const isActive = t.is_active;
        const updatedStr = t.updated_at ? formatConvTime(t.updated_at) : '';
        
        html += `
        <div style="border: 1px solid ${isActive ? 'var(--primary)' : 'var(--border)'}; border-radius: 8px; padding: 14px; margin-bottom: 8px; ${isActive ? 'background: var(--bg-card); box-shadow: 0 0 0 1px var(--primary);' : ''}">
            <div style="display: flex; justify-content: space-between; align-items: center; margin-bottom: 8px;">
                <div style="display: flex; align-items: center; gap: 8px;">
                    <span style="font-weight: 600; font-size: 15px;">${t.session_id}</span>
                    ${isActive ? '<span style="background: var(--primary); color: white; font-size: 11px; padding: 2px 8px; border-radius: 10px;">活跃</span>' : ''}
                </div>
                <div style="display: flex; gap: 6px;">
                    <button class="btn btn-sm" onclick="renameThread('${t.session_id}')">改名</button>
                    <button class="btn btn-sm" onclick="openThreadMemoryModal('${t.session_id}')">记忆</button>
                    ${!isActive ? `<button class="btn btn-sm btn-primary" onclick="switchThread('${t.session_id}')">切换到此</button>` : ''}
                    ${!isActive ? `<button class="btn btn-sm" onclick="deleteThread('${t.session_id}', ${t.message_count || 0})" style="color: var(--error);">删除</button>` : ''}
                </div>
            </div>
            <div style="color: var(--text-muted); font-size: 13px; line-height: 1.5;">
                <div style="display: flex; gap: 16px;">
                    <span>${t.message_count} 条消息</span>
                    ${updatedStr ? `<span>更新于 ${updatedStr}</span>` : ''}
                </div>
            </div>
        </div>`;
    }
    
    el.innerHTML = html;
}

async function createThread() {
    const newId = document.getElementById('new-thread-id').value.trim();
    const msgEl = document.getElementById('thread-create-msg');
    
    if (!newId) {
        msgEl.innerHTML = '<div style="color: var(--danger);">请输入对话线ID</div>';
        return;
    }
    
    try {
        const resp = await fetch('/api/partition/thread', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ session_id: newId })
        });
        const data = await resp.json();
        if (data.error) {
            msgEl.innerHTML = `<div style="color: var(--danger);">${data.error}</div>`;
            return;
        }
        
        msgEl.innerHTML = `<div style="color: var(--success);">创建成功</div>`;
        document.getElementById('new-thread-id').value = '';
        loadThreads();
    } catch(e) {
        msgEl.innerHTML = `<div style="color: var(--danger);">请求失败: ${e.message}</div>`;
    }
}

async function renameThread(oldId) {
    const newId = prompt(`请输入新的对话线ID（当前: ${oldId}）:`, oldId);
    if (!newId || newId.trim() === '' || newId.trim() === oldId) return;
    try {
        const resp = await fetch('/api/partition/thread/rename', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ old_id: oldId, new_id: newId.trim() })
        });
        const data = await resp.json();
        if (data.error) {
            alert('改名失败: ' + data.error);
            return;
        }
        loadThreads();
    } catch(e) {
        alert('请求失败: ' + e.message);
    }
}

async function switchThread(sessionId) {
    if (!confirm(`确定切换到对话线「${sessionId}」吗？\n\n切换后所有平台的新消息将存入此对话线。`)) return;
    
    try {
        const resp = await fetch('/api/partition/switch', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ session_id: sessionId })
        });
        const data = await resp.json();
        if (data.error) {
            alert('切换失败: ' + data.error);
            return;
        }
        loadThreads();
    } catch(e) {
        alert('请求失败: ' + e.message);
    }
}

async function deleteThread(sessionId, messageCount) {
    let msg;
    if (messageCount > 0) {
        msg = `⚠️ 对话线「${sessionId}」包含 ${messageCount} 条消息。\n\n删除后对话线配置和摘要将被移除，消息本身不受影响但会失去对话线归属。\n\n确定删除？`;
    } else {
        msg = `确定删除对话线「${sessionId}」吗？\n\n这只会删除对话线配置和摘要。`;
    }
    if (!confirm(msg)) return;
    
    try {
        const resp = await fetch('/api/partition/thread/' + encodeURIComponent(sessionId), { method: 'DELETE' });
        const data = await resp.json();
        if (data.error) {
            alert('删除失败: ' + data.error);
            return;
        }
        loadThreads();
    } catch(e) {
        alert('请求失败: ' + e.message);
    }
}

async function openSummaryModal(sessionId) {
    _summaryEditSid = sessionId;
    const titleEl = document.getElementById('summary-modal-title');
    const editor = document.getElementById('summary-editor');
    const saveBtn = document.getElementById('summary-save-btn');
    const clearBtn = document.getElementById('summary-clear-btn');
    if (titleEl) titleEl.textContent = '对话线记忆';
    const sidEl = document.getElementById('summary-modal-sid');
    if (sidEl) sidEl.textContent = sessionId;
    if (editor) { editor.readOnly = false; editor.onscroll = null; }
    _threadMemoryState = {sessionId: '', nodes: [], total: 0, offset: 0, hasMore: false, loading: false};
    if (saveBtn) saveBtn.style.display = '';
    if (clearBtn) clearBtn.style.display = '';
    
    // 获取完整摘要
    try {
        const resp = await fetch('/api/partition/status');
        const status = await resp.json();
        
        // 如果是活跃session就直接用status的摘要，否则单独获取
        let summary = '';
        if (sessionId === status.active_session_id) {
            summary = status.summary || '';
        } else {
            // 找对应thread的摘要
            const thread = _threadData.threads.find(t => t.session_id === sessionId);
            if (thread) summary = thread.summary || '';
        }
        
        document.getElementById('summary-editor').value = summary;
        updateSummaryCharCount();
        document.getElementById('summaryModal').style.display = 'flex';
    } catch(e) {
        alert('获取摘要失败: ' + e.message);
    }
}



const THREAD_MEMORY_PAGE_SIZE = 20;
let _threadMemoryState = {sessionId: '', nodes: [], total: 0, offset: 0, hasMore: false, loading: false};

function formatThreadMemoryNode(n) {
    const lines = [];
    const dateText = String(n.date || n.created_at || '').slice(0, 10);
    const tags = n.tags ? '｜标签:' + n.tags : '';
    const pin = n.pinned_until ? '｜📌便利贴到 ' + String(n.pinned_until).slice(0, 10) : '';
    lines.push('【' + (n.room || 'living_room') + '】' + dateText + '｜重要性:' + (n.importance || 5) + '｜情绪:' + (n.mood || 'neutral') + tags + pin);
    lines.push(String(n.content || '').trim());
    return lines.join('\n');
}

function renderThreadMemoryModalContent() {
    const editor = document.getElementById('summary-editor');
    const countEl = document.getElementById('summary-char-count');
    if (!editor) return;
    const nodes = _threadMemoryState.nodes || [];
    const total = Number(_threadMemoryState.total || nodes.length || 0);
    if (!nodes.length) {
        editor.value = '这个对话线还没有导入记忆宫殿。\n\n可以去「对话记录」勾选该对话，然后点击「提取记忆」，预览后导入。';
        if (countEl) countEl.textContent = '0 条记忆';
        return;
    }
    const lines = [];
    for (const n of nodes) {
        lines.push(formatThreadMemoryNode(n));
        lines.push('');
    }
    if (_threadMemoryState.loading) {
        lines.push('正在加载更多...');
    } else if (_threadMemoryState.hasMore) {
        lines.push('—— 继续向下滚动加载更多 ——');
    } else {
        lines.push('—— 已显示全部 ——');
    }
    editor.value = lines.join('\n');
    if (countEl) countEl.textContent = nodes.length + ' / ' + total + ' 条记忆';
}

async function loadThreadMemoryPage(sessionId, append) {
    const editor = document.getElementById('summary-editor');
    if (_threadMemoryState.loading) return;
    _threadMemoryState.loading = true;
    if (append) renderThreadMemoryModalContent();
    try {
        const offset = append ? _threadMemoryState.nodes.length : 0;
        const url = '/api/memory-palace/session-nodes?session_id=' + encodeURIComponent(sessionId) +
            '&character_id=default&limit=' + THREAD_MEMORY_PAGE_SIZE + '&offset=' + offset;
        const resp = await fetch(url);
        const data = await resp.json();
        if (!editor) return;
        if (data.error || data.status === 'error') {
            editor.value = '加载失败：' + (data.error || '未知错误');
            return;
        }
        const nodes = data.nodes || [];
        if (append) {
            _threadMemoryState.nodes = _threadMemoryState.nodes.concat(nodes);
        } else {
            _threadMemoryState.nodes = nodes;
        }
        _threadMemoryState.total = Number(data.total || _threadMemoryState.nodes.length || 0);
        _threadMemoryState.offset = Number(data.offset || offset || 0) + nodes.length;
        _threadMemoryState.hasMore = !!data.has_more;
        _threadMemoryState.sessionId = sessionId;
    } catch(e) {
        if (editor) editor.value = '请求失败：' + e.message;
    } finally {
        _threadMemoryState.loading = false;
        renderThreadMemoryModalContent();
    }
}

async function openThreadMemoryModal(sessionId) {
    _summaryEditSid = sessionId;
    _threadMemoryState = {sessionId: sessionId, nodes: [], total: 0, offset: 0, hasMore: false, loading: false};
    const titleEl = document.getElementById('summary-modal-title');
    const editor = document.getElementById('summary-editor');
    const countEl = document.getElementById('summary-char-count');
    const modal = document.getElementById('summaryModal');
    const saveBtn = document.getElementById('summary-save-btn');
    const clearBtn = document.getElementById('summary-clear-btn');
    if (titleEl) titleEl.textContent = '对话线记忆 — ' + sessionId;
    if (editor) {
        editor.value = '正在加载记忆宫殿内容...';
        editor.readOnly = true;
        editor.onscroll = function() {
            if (_summaryEditSid !== sessionId) return;
            if (!_threadMemoryState.hasMore || _threadMemoryState.loading) return;
            const distance = editor.scrollHeight - (editor.scrollTop + editor.clientHeight);
            if (distance < 80) loadThreadMemoryPage(sessionId, true);
        };
    }
    if (countEl) countEl.textContent = '';
    if (modal) modal.style.display = 'flex';
    if (saveBtn) saveBtn.style.display = 'none';
    if (clearBtn) clearBtn.style.display = 'none';
    await loadThreadMemoryPage(sessionId, false);
}

function closeSummaryModal() {
    document.getElementById('summaryModal').style.display = 'none';
    const editor = document.getElementById('summary-editor');
    if (editor) editor.readOnly = false;
    const titleEl = document.getElementById('summary-modal-title');
    const saveBtn = document.getElementById('summary-save-btn');
    const clearBtn = document.getElementById('summary-clear-btn');
    if (titleEl) titleEl.textContent = '对话线记忆';
    if (saveBtn) saveBtn.style.display = '';
    if (clearBtn) clearBtn.style.display = '';
    _summaryEditSid = '';
}

function updateSummaryCharCount() {
    const text = document.getElementById('summary-editor').value;
    document.getElementById('summary-char-count').textContent = `${text.length} 字`;
}

// 绑定输入事件
document.addEventListener('DOMContentLoaded', () => {
    const editor = document.getElementById('summary-editor');
    if (editor) editor.addEventListener('input', updateSummaryCharCount);
});

async function saveSummary() {
    const summary = document.getElementById('summary-editor').value;
    
    try {
        const resp = await fetch('/api/partition/summary', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ session_id: _summaryEditSid, summary: summary })
        });
        const data = await resp.json();
        if (data.error) {
            alert('保存失败: ' + data.error);
            return;
        }
        
        closeSummaryModal();
        loadThreads();
    } catch(e) {
        alert('请求失败: ' + e.message);
    }
}

async function clearSummary() {
    if (!confirm(`确定清空「${_summaryEditSid}」的摘要吗？此操作不可撤销。`)) return;
    
    try {
        const resp = await fetch('/api/partition/summary', {
            method: 'DELETE',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ session_id: _summaryEditSid })
        });
        const data = await resp.json();
        if (data.error) {
            alert('清空失败: ' + data.error);
            return;
        }
        
        closeSummaryModal();
        loadThreads();
    } catch(e) {
        alert('请求失败: ' + e.message);
    }
}

// ============================================
// 设置面板
// ============================================

let _settingsLoaded = false;
let _modelList = [];

// 所有需要读写的字段 key（开源版：EMBEDDING_API_KEY + EMBEDDING_BASE_URL）
const _SETTINGS_FIELDS = {
    str: ['API_BASE_URL', 'API_KEY', 'DEFAULT_MODEL', 'MEMORY_API_KEY', 'MEMORY_API_BASE_URL', 'MEMORY_MODEL',
          'CACHE_SUMMARY_MODEL', 'CACHE_PARTITION_TRIGGER', 'EMBEDDING_API_KEY', 'EMBEDDING_BASE_URL', 'EMBEDDING_MODEL', 'REASONING_EFFORT', 'USER_NICKNAME', 'CHARACTER_NAME'],
    int: ['MEMORY_PALACE_DEFAULT_LIMIT', 'MEMORY_PALACE_INJECTION_DEPTH', 'CACHE_PARTITION_X', 'CACHE_PARTITION_EXTRACT_LIMIT', 'CACHE_PARTITION_WINDOW', 'EMBEDDING_DIM'],
    float: [],
    optionalFloat: ['CHAT_TEMPERATURE'],
    bool: ['MEMORY_ENABLED', 'KEYWORD_CONTEXT_ENABLED', 'CACHE_PARTITION_ENABLED', 'CACHE_PARTITION_KEEP_A_TOOLS', 'TOOL_CHAIN_DEBUG', 'FORCE_STREAM', 'PERF_DIAGNOSTIC_ENABLED', 'RESPONSE_TRANSFORM_ENABLED'],
    range: [],
    text: ['systemPrompt', 'dailyImpressionPrompt', 'RESPONSE_TRANSFORM_RULES'],
};

const _MODEL_COMBOS = ['DEFAULT_MODEL', 'MEMORY_MODEL', 'CACHE_SUMMARY_MODEL'];

// 触发模式联动：time模式才显示时间窗口字段
function _togglePartitionWindow(trigger) {
    const el = document.getElementById('field-CACHE_PARTITION_WINDOW');
    if (el) el.style.display = trigger === 'time' ? '' : 'none';
}

async function loadSettings() {
    try {
        const resp = await fetch('/api/settings');
        const data = await resp.json();
        if (data.error) { showSettingsMsg('error', '加载失败: ' + data.error); return; }
        const s = data.settings;

        // 字符串字段
        _SETTINGS_FIELDS.str.forEach(k => {
            const el = document.getElementById('set-' + k);
            if (el) el.value = s[k] || '';
        });
        // 打码字段提示
        ['API_KEY', 'MEMORY_API_KEY', 'EMBEDDING_API_KEY'].forEach(k => {
            const hint = document.getElementById('set-' + k + '-hint');
            if (hint && s[k]) hint.textContent = '当前: ' + s[k];
        });
        // 整数
        _SETTINGS_FIELDS.int.forEach(k => {
            const el = document.getElementById('set-' + k);
            if (el) el.value = s[k];
        });
        // 浮点
        _SETTINGS_FIELDS.float.forEach(k => {
            const el = document.getElementById('set-' + k);
            if (el) el.value = s[k];
        });
        // 可留空浮点
        (_SETTINGS_FIELDS.optionalFloat || []).forEach(k => {
            const el = document.getElementById('set-' + k);
            if (el) el.value = s[k] === undefined || s[k] === null ? '' : s[k];
        });
        // 布尔（checkbox）
        _SETTINGS_FIELDS.bool.forEach(k => {
            const el = document.getElementById('set-' + k);
            if (el) el.checked = !!s[k];
        });
        // 滑块
        _SETTINGS_FIELDS.range.forEach(k => {
            const el = document.getElementById('set-' + k);
            if (el) { el.value = s[k]; updateSliderVal(k); }
        });
        // 长文本
        _SETTINGS_FIELDS.text.forEach(k => {
            const el = document.getElementById('set-' + k);
            if (el) el.value = s[k] || '';
        });
        updatePromptCount();
        // REASONING_EFFORT 下拉
        const reEl = document.getElementById('set-REASONING_EFFORT');
        if (reEl) reEl.value = s.REASONING_EFFORT || '';

        // CACHE_PARTITION_TRIGGER 下拉 + 联动时间窗口字段
        const triggerEl = document.getElementById('set-CACHE_PARTITION_TRIGGER');
        if (triggerEl) {
            triggerEl.value = s.CACHE_PARTITION_TRIGGER || 'rounds';
            _togglePartitionWindow(triggerEl.value);
            triggerEl.onchange = () => _togglePartitionWindow(triggerEl.value);
        }

        loadKeywordRulesEditor(s.KEYWORD_CONTEXT_RULES || '[]');

        // 加载模型列表（首次）
        if (!_settingsLoaded) loadModelList();
        _settingsLoaded = true;
    } catch (e) {
        showSettingsMsg('error', '加载设置失败: ' + e.message);
    }
}

async function saveSettings() {
    const btn = document.getElementById('save-settings-btn');
    btn.disabled = true;
    btn.textContent = '保存中...';

    const payload = {};
    syncKeywordRulesToHidden();
    const keywordRulesEl = document.getElementById('set-KEYWORD_CONTEXT_RULES');
    if (keywordRulesEl) payload.KEYWORD_CONTEXT_RULES = keywordRulesEl.value || '[]';

    // 字符串
    _SETTINGS_FIELDS.str.forEach(k => {
        const el = document.getElementById('set-' + k);
        if (el) payload[k] = el.value;
    });
    // 整数
    _SETTINGS_FIELDS.int.forEach(k => {
        const el = document.getElementById('set-' + k);
        if (el) payload[k] = parseInt(el.value) || 0;
    });
    // 浮点
    _SETTINGS_FIELDS.float.forEach(k => {
        const el = document.getElementById('set-' + k);
        if (el) payload[k] = parseFloat(el.value) || 0;
    });
    // 可留空浮点
    (_SETTINGS_FIELDS.optionalFloat || []).forEach(k => {
        const el = document.getElementById('set-' + k);
        if (el) payload[k] = el.value.trim() === '' ? '' : parseFloat(el.value);
    });
    // 布尔
    _SETTINGS_FIELDS.bool.forEach(k => {
        const el = document.getElementById('set-' + k);
        if (el) payload[k] = el.checked;
    });
    // 滑块
    _SETTINGS_FIELDS.range.forEach(k => {
        const el = document.getElementById('set-' + k);
        if (el) payload[k] = parseFloat(el.value) || 0;
    });
    // 长文本
    _SETTINGS_FIELDS.text.forEach(k => {
        const el = document.getElementById('set-' + k);
        if (el) payload[k] = el.value;
    });

    try {
        const resp = await fetch('/api/settings', {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });
        const data = await resp.json();
        if (data.error) {
            showSettingsMsg('error', '保存失败: ' + data.error);
        } else {
            const msg = `已更新 ${data.updated?.length || 0} 项` +
                        (data.skipped?.length ? `，跳过 ${data.skipped.length} 项（未修改）` : '');
            showSettingsMsg('success', msg);
        }
    } catch (e) {
        showSettingsMsg('error', '保存失败: ' + e.message);
    } finally {
        btn.disabled = false;
        btn.textContent = '保存设置';
    }
}

async function testMemoryModel() {
    const btn = document.getElementById('test-memory-model-btn');
    const result = document.getElementById('test-memory-model-result');
    if (btn) { btn.disabled = true; btn.textContent = '测试中...'; }
    if (result) result.textContent = '正在发送测试请求...';

    const payload = {
        MEMORY_API_BASE_URL: document.getElementById('set-MEMORY_API_BASE_URL')?.value || '',
        MEMORY_API_KEY: document.getElementById('set-MEMORY_API_KEY')?.value || '',
        MEMORY_MODEL: document.getElementById('set-MEMORY_MODEL')?.value || '',
    };

    try {
        const resp = await fetch('/api/settings/test-memory-model', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload),
        });
        const data = await resp.json();
        if (data.ok) {
            if (result) result.textContent = '✅ 测试成功：' + (data.reply || '接口可用');
            showSettingsMsg('success', '记忆模型测试成功');
        } else {
            if (result) result.textContent = '❌ 测试失败：' + (data.error || '未知错误');
            showSettingsMsg('error', '记忆模型测试失败');
        }
    } catch (e) {
        if (result) result.textContent = '❌ 请求失败：' + e.message;
        showSettingsMsg('error', '测试请求失败: ' + e.message);
    } finally {
        if (btn) { btn.disabled = false; btn.textContent = '测试记忆模型'; }
    }
}

async function loadModelList() {
    const hint = document.getElementById('model-count-hint');
    if (hint) hint.textContent = '加载模型列表...';
    try {
        const resp = await fetch('/api/models');
        const data = await resp.json();
        _modelList = data.models || [];

        _MODEL_COMBOS.forEach(fieldName => {
            renderComboDropdown(fieldName, _modelList);
        });

        if (hint) {
            hint.textContent = _modelList.length > 0
                ? `共 ${_modelList.length} 个可用模型 (${data.provider || ''})`
                : '无法获取模型列表，请手动输入';
        }
    } catch (e) {
        if (hint) hint.textContent = '模型列表加载失败';
    }
}

function renderComboDropdown(fieldName, models) {
    const dropdown = document.getElementById('dropdown-' + fieldName);
    if (!dropdown) return;
    dropdown.innerHTML = '';
    models.forEach(m => {
        const div = document.createElement('div');
        div.className = 'combo-option';
        div.textContent = m.name || m.id;
        div.dataset.value = m.id;
        div.addEventListener('click', () => {
            document.getElementById('set-' + fieldName).value = m.id;
            dropdown.classList.remove('open');
        });
        dropdown.appendChild(div);
    });
}

function filterCombo(fieldName) {
    const input = document.getElementById('set-' + fieldName);
    const dropdown = document.getElementById('dropdown-' + fieldName);
    if (!input || !dropdown) return;
    const q = input.value.toLowerCase();
    let visible = 0;
    dropdown.querySelectorAll('.combo-option').forEach(opt => {
        const match = !q || opt.textContent.toLowerCase().includes(q) || (opt.dataset.value || '').toLowerCase().includes(q);
        opt.style.display = match ? '' : 'none';
        if (match) visible++;
    });
    if (visible > 0 && q) dropdown.classList.add('open');
}

// 初始化 combo-box 交互
document.addEventListener('DOMContentLoaded', () => {
    _MODEL_COMBOS.forEach(fieldName => {
        const input = document.getElementById('set-' + fieldName);
        const dropdown = document.getElementById('dropdown-' + fieldName);
        if (!input || !dropdown) return;

        input.addEventListener('focus', () => { dropdown.classList.add('open'); });
        input.addEventListener('input', () => { filterCombo(fieldName); });
    });

    // 点击外部关闭所有 combo
    document.addEventListener('click', (e) => {
        _MODEL_COMBOS.forEach(fieldName => {
            const box = document.getElementById('combo-' + fieldName);
            const dropdown = document.getElementById('dropdown-' + fieldName);
            if (box && dropdown && !box.contains(e.target)) {
                dropdown.classList.remove('open');
            }
        });
    });
});

function updateSliderVal(key) {
    const el = document.getElementById('set-' + key);
    const span = document.getElementById('val-' + key);
    if (el && span) span.textContent = parseFloat(el.value).toFixed(2);
}

function updatePromptCount() {
    const el = document.getElementById('set-systemPrompt');
    const hint = document.getElementById('prompt-char-count');
    if (el && hint) hint.textContent = el.value.length + ' 字';
}

// ============================================
// 关键词触发上下文规则编辑器
// ============================================
let _keywordRules = [];
let _keywordRuleExpanded = [];

function normalizeKeywordRule(rule) {
    rule = rule || {};
    let keywords = rule.keywords || [];
    if (typeof keywords === 'string') keywords = keywords.split(',');
    keywords = Array.isArray(keywords) ? keywords.map(k => String(k).trim()).filter(Boolean) : [];
    return {
        enabled: rule.enabled !== false,
        name: String(rule.name || '未命名规则'),
        keywords,
        match: rule.match === 'exact' ? 'exact' : 'contains',
        content: String(rule.content || '')
    };
}

function loadKeywordRulesEditor(raw) {
    let rules = [];
    try {
        const parsed = JSON.parse(raw || '[]');
        rules = Array.isArray(parsed) ? parsed : (Array.isArray(parsed.rules) ? parsed.rules : []);
    } catch(e) {
        rules = [];
        const result = document.getElementById('keyword-rule-test-result');
        if (result) result.textContent = '规则 JSON 解析失败，已显示为空列表：' + e.message;
    }
    _keywordRules = rules.map(normalizeKeywordRule);
    _keywordRuleExpanded = _keywordRules.map(() => false);
    renderKeywordRulesEditor();
}

function renderKeywordRulesEditor() {
    const box = document.getElementById('keyword-rule-editor');
    if (!box) return;
    if (!_keywordRules.length) {
        box.innerHTML = '<div class="form-hint" style="padding:12px;border:1px dashed var(--border);border-radius:8px;">暂无规则，点击“添加规则”开始。</div>';
        syncKeywordRulesToHidden();
        return;
    }
    box.innerHTML = _keywordRules.map((r, i) => {
        const expanded = !!_keywordRuleExpanded[i];
        const kwSummary = r.keywords.length ? r.keywords.slice(0, 5).join('、') + (r.keywords.length > 5 ? '…' : '') : '未设置关键词';
        const contentSummary = String(r.content || '').trim().replace(/\s+/g, ' ').slice(0, 80);
        return `
        <div class="card" style="padding:12px;margin:10px 0;border-left:4px solid ${r.enabled ? 'var(--primary)' : 'var(--border)'};">
            <div style="display:flex;gap:8px;align-items:center;justify-content:space-between;flex-wrap:wrap;">
                <div style="min-width:220px;flex:1;">
                    <div style="font-weight:600;">${escHtml(r.name || '未命名规则')}</div>
                    <div class="form-hint" style="margin-top:3px;">${r.enabled ? '已启用' : '已禁用'} · ${r.match === 'exact' ? '完全匹配' : '包含关键词'} · ${escHtml(kwSummary)}</div>
                    ${contentSummary ? `<div class="form-hint" style="margin-top:3px;">${escHtml(contentSummary)}${String(r.content || '').length > 80 ? '…' : ''}</div>` : ''}
                </div>
                <div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">
                    <label class="checkbox-label"><input type="checkbox" ${r.enabled ? 'checked' : ''} onchange="updateKeywordRule(${i}, 'enabled', this.checked); renderKeywordRulesEditor();"><span>启用</span></label>
                    <button type="button" class="btn btn-sm" onclick="toggleKeywordRule(${i})">${expanded ? '收起' : '展开'}</button>
                    <button type="button" class="btn btn-sm" style="color:var(--danger);" onclick="deleteKeywordRule(${i})">删除</button>
                </div>
            </div>
            <div style="display:${expanded ? 'block' : 'none'};margin-top:10px;">
                <div class="settings-field"><label>规则名</label><input class="input" value="${escHtml(r.name)}" oninput="updateKeywordRule(${i}, 'name', this.value)"></div>
                <div class="settings-field"><label>关键词（用逗号分隔）</label><input class="input" value="${escHtml(r.keywords.join(', '))}" oninput="updateKeywordRule(${i}, 'keywordsText', this.value)"></div>
                <div class="settings-field"><label>匹配方式</label><select class="input" onchange="updateKeywordRule(${i}, 'match', this.value)"><option value="contains" ${r.match !== 'exact' ? 'selected' : ''}>包含关键词</option><option value="exact" ${r.match === 'exact' ? 'selected' : ''}>完全匹配</option></select></div>
                <div class="settings-field"><label>触发后注入内容</label><textarea class="textarea" rows="5" oninput="updateKeywordRule(${i}, 'content', this.value)">${escHtml(r.content)}</textarea></div>
            </div>
        </div>
    `}).join('');
    syncKeywordRulesToHidden();
}

function updateKeywordRule(index, field, value) {
    if (!_keywordRules[index]) return;
    if (field === 'keywordsText') {
        _keywordRules[index].keywords = String(value || '').split(',').map(k => k.trim()).filter(Boolean);
    } else {
        _keywordRules[index][field] = value;
    }
    syncKeywordRulesToHidden();
}

function addKeywordRule() {
    _keywordRules.push({enabled: true, name: '新规则', keywords: ['关键词'], match: 'contains', content: '这里填写命中后本轮临时注入的系统上下文。'});
    _keywordRuleExpanded = _keywordRules.map((_, i) => i === _keywordRules.length - 1 ? true : !!_keywordRuleExpanded[i]);
    renderKeywordRulesEditor();
}

function toggleKeywordRule(index) {
    _keywordRuleExpanded[index] = !_keywordRuleExpanded[index];
    renderKeywordRulesEditor();
}

function deleteKeywordRule(index) {
    if (!confirm('删除这条关键词规则？')) return;
    _keywordRules.splice(index, 1);
    _keywordRuleExpanded.splice(index, 1);
    renderKeywordRulesEditor();
}

function syncKeywordRulesToHidden() {
    const el = document.getElementById('set-KEYWORD_CONTEXT_RULES');
    if (el) el.value = JSON.stringify(_keywordRules.map(normalizeKeywordRule), null, 2);
}

function testKeywordRules() {
    syncKeywordRulesToHidden();
    const input = document.getElementById('keyword-rule-test-input');
    const result = document.getElementById('keyword-rule-test-result');
    const text = input ? input.value : '';
    const q = String(text || '');
    const qLower = q.toLowerCase();
    const hits = _keywordRules.filter(r => r.enabled && r.keywords.some(k => {
        k = String(k || '').trim();
        if (!k) return false;
        return r.match === 'exact' ? q.trim() === k : qLower.includes(k.toLowerCase());
    }));
    if (result) result.textContent = hits.length ? ('会命中：' + hits.map(r => r.name).join('、')) : '不会命中任何规则';
}

window.addKeywordRule = addKeywordRule;
window.deleteKeywordRule = deleteKeywordRule;
window.updateKeywordRule = updateKeywordRule;
window.toggleKeywordRule = toggleKeywordRule;
window.testKeywordRules = testKeywordRules;

// 绑定 prompt 字数实时更新
document.addEventListener('DOMContentLoaded', () => {
    const p = document.getElementById('set-systemPrompt');
    if (p) p.addEventListener('input', updatePromptCount);
});

function showSettingsMsg(type, text) {
    const el = document.getElementById('settings-msg');
    if (!el) return;
    el.style.display = 'block';
    el.className = 'msg-box msg-' + type;
    el.textContent = text;
    setTimeout(() => { el.style.display = 'none'; }, 5000);
}



// ============================================
// 用户画像 / 印象档案
// ============================================

let _userImpressionCurrent = null;
let _userImpressionPreview = null;
let _userImpressionPreviewAbortController = null;
let _userImpressionGenerating = false;
let _userImpressionPreviewRequestId = 0;
let _userImpressionEditOpen = false;

function uiEsc(v) {
    return String(v === undefined || v === null ? '' : v)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '"')
        .replace(/'/g, '&#39;');
}
function showUserImpressionMsg(type, text) {
    const el = document.getElementById('ui-msg');
    if (!el) return;
    el.innerHTML = '<div class="msg-box msg-' + type + '">' + uiEsc(text) + '</div>';
    setTimeout(() => { if (el) el.innerHTML = ''; }, 5000);
}

function uiNoteHeading(title, tone) {
    const bg = tone === 'blue' ? 'rgba(191,211,232,.62)' : (tone === 'gray' ? 'rgba(218,218,218,.62)' : 'rgba(238,199,214,.62)');
    return '<div style="margin-bottom:10px;"><span style="display:inline-block;padding:2px 10px 3px 10px;border-radius:999px;background:' + bg + ';font-weight:900;font-size:13px;letter-spacing:.03em;color:#5f6670;box-decoration-break:clone;-webkit-box-decoration-break:clone;">' + uiEsc(title) + '</span></div>';
}

function uiNotebookCard(inner, extraStyle) {
    return '<div class="card" style="box-shadow:none;border:1px solid rgba(156,163,175,.34);padding:14px;margin:0;background:linear-gradient(to bottom, rgba(255,255,255,.88), rgba(255,255,255,.88)),repeating-linear-gradient(to bottom, transparent 0, transparent 27px, rgba(148,163,184,.18) 28px);border-radius:16px;' + (extraStyle || '') + '">' + inner + '</div>';
}

function uiListHtml(items) {
    const arr = Array.isArray(items) ? items.filter(x => String(x || '').trim()) : [];
    if (!arr.length) return '<span style="color:var(--text-muted);font-size:13px;">暂无</span>';
    return '<div style="display:flex;flex-wrap:wrap;gap:7px;">' + arr.map((t, i) => '<span style="padding:4px 9px;border-radius:999px;background:' + (i % 2 ? 'rgba(191,211,232,.45)' : 'rgba(238,199,214,.45)') + ';border:1px solid rgba(148,163,184,.22);font-size:12px;color:#5f6670;">' + uiEsc(t) + '</span>').join('') + '</div>';
}

function uiTextBlock(title, text) {
    return uiNotebookCard(
        uiNoteHeading(title, 'pink') +
        '<div style="font-size:14px;line-height:1.85;white-space:pre-wrap;color:#4f5660;">' + uiEsc(text || '暂无') + '</div>'
    );
}

function renderUserImpressionObject(imp) {
    if (!imp) {
        return '<div style="color:var(--text-muted);padding:10px;">尚未生成用户画像。可以点击“生成画像”先生成预览。</div>';
    }
    const value = imp.value_map || {};
    const behavior = imp.behavior_profile || {};
    const emotion = imp.emotion_schema || {};
    const triggers = emotion.triggers || {};
    const core = imp.personality_core || {};
    const mbti = imp.mbti_analysis || null;
    const dims = (mbti && mbti.dimensions) || {};
    const changes = imp.observed_changes || [];
    let html = '<div style="position:relative;padding:14px 12px 16px 28px;border:1px solid rgba(148,163,184,.35);border-radius:20px;background:linear-gradient(180deg,rgba(255,247,250,.72),rgba(245,250,255,.58));overflow:hidden;">' +
        '<div style="position:absolute;left:12px;top:18px;bottom:18px;width:2px;background:rgba(148,163,184,.38);"></div>' +
        '<div style="position:absolute;left:6px;top:34px;width:14px;height:14px;border:2px solid rgba(148,163,184,.45);border-radius:50%;background:white;"></div>' +
        '<div style="position:absolute;left:6px;top:92px;width:14px;height:14px;border:2px solid rgba(148,163,184,.45);border-radius:50%;background:white;"></div>' +
        '<div style="position:absolute;left:6px;top:150px;width:14px;height:14px;border:2px solid rgba(148,163,184,.45);border-radius:50%;background:white;"></div>';

    html += '<div class="card" style="box-shadow:none;border:1px solid rgba(156,163,175,.34);padding:0;margin:0;overflow:hidden;background:linear-gradient(to bottom, rgba(255,255,255,.90), rgba(255,255,255,.90)),repeating-linear-gradient(to bottom, transparent 0, transparent 28px, rgba(148,163,184,.18) 29px);border-radius:16px;">' +
        '<div style="padding:12px 16px;border-bottom:1px solid rgba(156,163,175,.30);background:rgba(238,199,214,.32);display:flex;justify-content:space-between;gap:10px;align-items:center;">' +
        '<div style="font-size:13px;font-weight:900;letter-spacing:.08em;color:#5f6670;"><span style="background:rgba(238,199,214,.65);border-radius:999px;padding:2px 10px;">核心印象</span></div>' +
        '<div style="font-size:11px;color:#8a9099;">Private Reader Note</div>' +
        '</div>' +
        '<div style="padding:18px 18px 16px 18px;">' +
        '<div style="font-size:16px;line-height:1.9;white-space:pre-wrap;color:#4f5660;font-family:Georgia,Times New Roman,serif;">' + uiEsc(core.summary || '暂无') + '</div>' +
        '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:10px;margin-top:16px;padding-top:14px;border-top:1px dashed rgba(148,163,184,.35);">' +
        '<div style="background:rgba(191,211,232,.26);border:1px solid rgba(148,163,184,.18);border-radius:12px;padding:10px;"><div style="font-size:11px;color:#6b7280;font-weight:800;margin-bottom:4px;">互动模式</div><div style="font-size:13px;line-height:1.6;color:#4f5660;">' + uiEsc(core.interaction_style || '暂无') + '</div></div>' +
        '<div style="background:rgba(238,199,214,.24);border:1px solid rgba(148,163,184,.18);border-radius:12px;padding:10px;"><div style="font-size:11px;color:#6b7280;font-weight:800;margin-bottom:4px;">语气感知</div><div style="font-size:13px;line-height:1.6;color:#4f5660;">' + uiEsc(behavior.tone_style || '暂无') + '</div></div>' +
        '</div>' +
        '</div></div>';

    html += uiNotebookCard(
        '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start;margin-bottom:10px;">' +
        uiNoteHeading('MBTI 侧写', 'blue') +
        '<div style="font-size:22px;font-weight:900;color:var(--primary);">' + uiEsc((mbti && mbti.type) || 'XXXX') + '</div>' +
        '</div>' +
        '<div style="font-size:12px;color:var(--text-muted);line-height:1.6;margin-bottom:10px;white-space:pre-wrap;">' + uiEsc((mbti && mbti.reasoning) || '暂无') + '</div>' +
        '<div style="font-size:12px;line-height:1.8;">E/I: ' + uiEsc(dims.e_i ?? 50) + ' · S/N: ' + uiEsc(dims.s_n ?? 50) + ' · T/F: ' + uiEsc(dims.t_f ?? 50) + ' · J/P: ' + uiEsc(dims.j_p ?? 50) + '</div>' +
        '</div>', 'margin-top:14px;');

    html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:14px;margin-top:14px;">';
    html += uiTextBlock('核心价值观', value.core_values);
    html += uiTextBlock('情绪状态总结', behavior.emotion_summary);
    html += uiTextBlock('回应模式', behavior.response_patterns);
    html += uiTextBlock('舒适区', emotion.comfort_zone);
    html += '</div>';

    html += uiNotebookCard(
        uiNoteHeading('价值地图', 'pink') +
        '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px;">' +
        '<div><div style="font-size:12px;font-weight:800;margin-bottom:8px;">观察到的特质</div>' + uiListHtml(core.observed_traits) + '</div>' +
        '<div><div style="font-size:12px;font-weight:800;margin-bottom:8px;">喜欢</div>' + uiListHtml(value.likes) + '</div>' +
        '<div><div style="font-size:12px;font-weight:800;margin-bottom:8px;">讨厌/排斥</div>' + uiListHtml(value.dislikes) + '</div>' +
        '</div></div>', 'margin-top:14px;');

    html += uiNotebookCard(
        uiNoteHeading('行为与情绪', 'blue') +
        '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px;">' +
        '<div><div style="font-size:12px;font-weight:800;margin-bottom:8px;">正向触发器</div>' + uiListHtml(triggers.positive) + '</div>' +
        '<div><div style="font-size:12px;font-weight:800;margin-bottom:8px;">压力/雷区</div>' + uiListHtml(triggers.negative) + '</div>' +
        '<div><div style="font-size:12px;font-weight:800;margin-bottom:8px;">压力信号</div>' + uiListHtml(emotion.stress_signals) + '</div>' +
        '</div></div>', 'margin-top:14px;');

    html += uiNotebookCard(
        uiNoteHeading('最近变化', 'gray') +
        uiListHtml(changes), 'margin-top:14px;'
    );

    html += '<div style="font-size:12px;color:var(--text-muted);margin-top:12px;">Version ' + uiEsc(imp.version || 3.0) + ' · lastUpdated ' + uiEsc(imp.lastUpdated || '') + '</div>';
    html += '</div>';
    return html;
}


function uiArrToText(items) {
    return Array.isArray(items) ? items.map(x => String(x || '').trim()).filter(Boolean).join('\n') : '';
}

function uiTextToArr(id) {
    const el = document.getElementById(id);
    if (!el) return [];
    return String(el.value || '').split(/\n+/).map(x => x.trim()).filter(Boolean);
}

function uiEditValue(id) {
    const el = document.getElementById(id);
    return el ? String(el.value || '').trim() : '';
}

function uiEditNum(id, fallback) {
    const n = parseInt(uiEditValue(id), 10);
    if (!Number.isFinite(n)) return fallback;
    return Math.max(0, Math.min(100, n));
}

function uiEditField(label, id, value, multiline) {
    if (multiline) {
        return '<label style="display:flex;flex-direction:column;gap:6px;font-size:12px;font-weight:800;">' +
            uiEsc(label) +
            '<textarea id="' + uiEsc(id) + '" class="textarea" rows="4" style="min-height:90px;">' + uiEsc(value || '') + '</textarea>' +
            '</label>';
    }
    return '<label style="display:flex;flex-direction:column;gap:6px;font-size:12px;font-weight:800;">' +
        uiEsc(label) +
        '<input id="' + uiEsc(id) + '" class="input" value="' + uiEsc(value || '') + '">' +
        '</label>';
}

function uiEditBlock(title, inner) {
    return '<div class="card" style="box-shadow:none;border:1px solid var(--border-color);padding:16px;margin:0;">' +
        '<div style="font-weight:900;margin-bottom:12px;">' + uiEsc(title) + '</div>' +
        '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px;">' + inner + '</div>' +
        '</div>';
}

function renderUserImpressionEditor(imp) {
    imp = imp || {};
    const value = imp.value_map || {};
    const behavior = imp.behavior_profile || {};
    const emotion = imp.emotion_schema || {};
    const triggers = emotion.triggers || {};
    const core = imp.personality_core || {};
    const mbti = imp.mbti_analysis || {};
    const dims = mbti.dimensions || {};

    let html = '<div id="uiFieldEditor" class="card" style="padding:18px;border:1px solid var(--primary);box-shadow:none;margin-top:14px;">';
    html += '<div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap;margin-bottom:14px;">' +
        '<div><div style="font-weight:900;">编辑画像字段</div><div style="font-size:12px;color:var(--text-muted);margin-top:2px;">数组字段一行一个。保存后会走后端 normalize 兜底。</div></div>' +
        '<div style="display:flex;gap:8px;flex-wrap:wrap;"><button class="btn btn-primary" onclick="saveUserImpressionEdit()">保存编辑</button><button class="btn btn-secondary" onclick="cancelUserImpressionEdit()">取消</button></div>' +
        '</div>';

    html += '<div style="display:grid;gap:14px;">';
    html += uiEditBlock('核心印象',
        uiEditField('核心评价 / summary', 'uiEdit_summary', core.summary, true) +
        uiEditField('互动模式 / interaction_style', 'uiEdit_interaction', core.interaction_style, true) +
        uiEditField('观察到的特质（一行一个）', 'uiEdit_traits', uiArrToText(core.observed_traits), true)
    );
    html += uiEditBlock('价值地图',
        uiEditField('喜欢（一行一个）', 'uiEdit_likes', uiArrToText(value.likes), true) +
        uiEditField('讨厌/排斥（一行一个）', 'uiEdit_dislikes', uiArrToText(value.dislikes), true) +
        uiEditField('核心价值观', 'uiEdit_core_values', value.core_values, true)
    );
    html += uiEditBlock('行为画像',
        uiEditField('语气风格', 'uiEdit_tone', behavior.tone_style, true) +
        uiEditField('情绪状态总结', 'uiEdit_emotion_summary', behavior.emotion_summary, true) +
        uiEditField('回应模式', 'uiEdit_response_patterns', behavior.response_patterns, true)
    );
    html += uiEditBlock('情绪图谱',
        uiEditField('正向触发器（一行一个）', 'uiEdit_positive', uiArrToText(triggers.positive), true) +
        uiEditField('压力/雷区（一行一个）', 'uiEdit_negative', uiArrToText(triggers.negative), true) +
        uiEditField('舒适区', 'uiEdit_comfort_zone', emotion.comfort_zone, true) +
        uiEditField('压力信号（一行一个）', 'uiEdit_stress', uiArrToText(emotion.stress_signals), true)
    );
    html += uiEditBlock('MBTI 侧写',
        uiEditField('类型', 'uiEdit_mbti_type', mbti.type || '', false) +
        uiEditField('推断理由', 'uiEdit_mbti_reasoning', mbti.reasoning || '', true) +
        uiEditField('E/I 数值 0-100', 'uiEdit_ei', dims.e_i ?? 50, false) +
        uiEditField('S/N 数值 0-100', 'uiEdit_sn', dims.s_n ?? 50, false) +
        uiEditField('T/F 数值 0-100', 'uiEdit_tf', dims.t_f ?? 50, false) +
        uiEditField('J/P 数值 0-100', 'uiEdit_jp', dims.j_p ?? 50, false)
    );
    html += uiEditBlock('最近变化',
        uiEditField('最近变化（一行一个）', 'uiEdit_changes', uiArrToText(imp.observed_changes), true)
    );
    html += '</div></div>';
    return html;
}

function openUserImpressionEditor() {
    if (!_userImpressionCurrent || !_userImpressionCurrent.impression) {
        showUserImpressionMsg('error', '当前没有可编辑的画像。');
        return;
    }
    _userImpressionEditOpen = true;
    renderUserImpressionEditorCard();
    const editor = document.getElementById('uiEditorCard');
    if (editor && editor.scrollIntoView) editor.scrollIntoView({behavior:'smooth', block:'start'});
}

function renderUserImpressionEditorCard() {
    const el = document.getElementById('uiEditorCard');
    if (!el) return;
    if (_userImpressionEditOpen && _userImpressionCurrent && _userImpressionCurrent.impression) {
        el.style.display = 'block';
        el.innerHTML = renderUserImpressionEditor(_userImpressionCurrent.impression);
    } else {
        el.style.display = 'none';
        el.innerHTML = '';
    }
}

function cancelUserImpressionEdit() {
    _userImpressionEditOpen = false;
    renderUserImpressionEditorCard();
}

function collectUserImpressionEdit() {
    const old = (_userImpressionCurrent && _userImpressionCurrent.impression) || {};
    return {
        version: parseFloat(old.version || 3.0) || 3.0,
        lastUpdated: Date.now(),
        value_map: {
            likes: uiTextToArr('uiEdit_likes'),
            dislikes: uiTextToArr('uiEdit_dislikes'),
            core_values: uiEditValue('uiEdit_core_values'),
        },
        behavior_profile: {
            tone_style: uiEditValue('uiEdit_tone'),
            emotion_summary: uiEditValue('uiEdit_emotion_summary'),
            response_patterns: uiEditValue('uiEdit_response_patterns'),
        },
        emotion_schema: {
            triggers: {
                positive: uiTextToArr('uiEdit_positive'),
                negative: uiTextToArr('uiEdit_negative'),
            },
            comfort_zone: uiEditValue('uiEdit_comfort_zone'),
            stress_signals: uiTextToArr('uiEdit_stress'),
        },
        personality_core: {
            observed_traits: uiTextToArr('uiEdit_traits'),
            interaction_style: uiEditValue('uiEdit_interaction'),
            summary: uiEditValue('uiEdit_summary'),
        },
        mbti_analysis: {
            type: uiEditValue('uiEdit_mbti_type'),
            reasoning: uiEditValue('uiEdit_mbti_reasoning'),
            dimensions: {
                e_i: uiEditNum('uiEdit_ei', 50),
                s_n: uiEditNum('uiEdit_sn', 50),
                t_f: uiEditNum('uiEdit_tf', 50),
                j_p: uiEditNum('uiEdit_jp', 50),
            },
        },
        observed_changes: uiTextToArr('uiEdit_changes'),
    };
}

async function saveUserImpressionEdit() {
    if (!_userImpressionCurrent || !_userImpressionCurrent.impression) {
        showUserImpressionMsg('error', '当前没有可保存的画像。');
        return;
    }
    try {
        const impression = collectUserImpressionEdit();
        const resp = await fetch('/api/user-impression/confirm', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({
                character_id: _userImpressionCurrent.character_id || 'default',
                mode: 'manual',
                source_message_count: _userImpressionCurrent.source_message_count || 0,
                impression
            })
        });
        const data = await resp.json();
        if (data.status !== 'ok') throw new Error(data.error || '保存失败');
        _userImpressionCurrent = data;
        _userImpressionEditOpen = false;
        renderUserImpression();
        showUserImpressionMsg('success', '画像编辑已保存。');
    } catch (e) {
        showUserImpressionMsg('error', '保存失败：' + e.message);
    }
}


function renderUserImpression() {
    const el = document.getElementById('uiCurrentImpression');
    if (!el) return;
    if (!_userImpressionCurrent) {
        el.innerHTML = '<div style="font-weight:800;margin-bottom:8px;">当前画像</div>' + renderUserImpressionObject(null);
        renderUserImpressionEditorCard();
        return;
    }
    el.innerHTML = '<div style="display:flex;justify-content:space-between;gap:12px;flex-wrap:wrap;margin-bottom:12px;">' +
        '<div><div style="font-weight:800;">当前画像</div><div style="font-size:12px;color:var(--text-muted);margin-top:2px;">更新时间：' + uiEsc(_userImpressionCurrent.updated_at || '') + ' · 来源：' + uiEsc(_userImpressionCurrent.source_mode || '') + (_userImpressionCurrent.pending_memory_count ? ' · 待处理记忆：' + _userImpressionCurrent.pending_memory_count + '条' : '') + '</div></div>' +
        '<div style="display:flex;gap:8px;flex-wrap:wrap;"><button class="btn btn-secondary" onclick="openUserImpressionEditor()">编辑画像</button></div>' +
        '</div>' +
        renderUserImpressionObject(_userImpressionCurrent.impression);
    renderUserImpressionEditorCard();
}

async function loadUserImpression() {
    const el = document.getElementById('uiCurrentImpression');
    // 已有数据时直接渲染，不闪加载中
    if (_userImpressionCurrent !== null) {
        renderUserImpression();
        return;
    }
    if (el) el.innerHTML = '加载中...';
    try {
        const resp = await fetch('/api/user-impression?character_id=default');
        const data = await resp.json();
        if (data.status === 'not_found') {
            _userImpressionCurrent = null;
        } else if (data.status === 'ok') {
            _userImpressionCurrent = data;
        } else if (data.error) {
            throw new Error(data.error);
        } else {
            _userImpressionCurrent = null;
        }
        renderUserImpression();
    } catch (e) {
        if (el) el.innerHTML = '<div class="msg-box msg-error">加载失败：' + uiEsc(e.message) + '</div>';
    }
}

async function previewUserImpressionMaterials() {
    const card = document.getElementById('uiPreviewCard');
    const content = document.getElementById('uiPreviewContent');
    const meta = document.getElementById('uiPreviewMeta');
    if (card) card.style.display = 'block';
    if (meta) meta.textContent = 'update 模式材料预览 · 不调用 LLM · 不保存';
    if (content) content.innerHTML = '正在加载画像材料预览...';
    try {
        const resp = await fetch('/api/user-impression/materials-preview', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({character_id:'default', mode:'update'})
        });
        const data = await resp.json();
        if (data.status !== 'ok') throw new Error(data.error || '材料预览失败');
        const text = data.material_text_full || data.material_text_preview || '';
        if (meta) {
            const mem = (data.memory_palace && data.memory_palace.count) || 0;
            const recent = (data.recent_messages && data.recent_messages.count) || 0;
            meta.textContent = 'update 模式材料预览 · 记忆 ' + mem + ' 条 · 近期聊天 ' + recent + ' 条 · 材料 ' + (data.material_text_chars || text.length || 0) + ' 字 · 不调用 LLM';
        }
        if (content) {
            content.textContent = '';
            const info = document.createElement('div');
            info.className = 'msg-box msg-info';
            info.style.marginBottom = '10px';
            info.textContent = '这是 update 模式实际进入画像生成的材料内容；本操作不调用 LLM、不保存。';
            const pre = document.createElement('pre');
            pre.style.cssText = 'white-space:pre-wrap;word-break:break-word;max-height:70vh;overflow:auto;background:#0f172a;color:#e5e7eb;border-radius:14px;padding:16px;font-size:12px;line-height:1.65;';
            pre.textContent = text || '（空）';
            content.appendChild(info);
            content.appendChild(pre);
        }
    } catch (e) {
        if (content) content.innerHTML = '<div class="msg-box msg-error">材料预览失败：' + uiEsc(e.message) + '</div>';
    }
}

function confirmGenerateUserImpressionPreview(mode) {
    const isUpdate = mode === 'update';
    const message = isUpdate
        ? '确定要追加/更新用户画像预览吗？\n\n这会带入当前画像，并读取更多近期聊天。生成结果只会进入预览，不会自动覆盖。'
        : '确定要从零生成新的用户画像预览吗？\n\n这不会读取旧画像。生成结果只会进入预览，不会自动覆盖。';
    if (!confirm(message)) return;
    generateUserImpressionPreview(isUpdate ? 'update' : 'initial');
}

async function generateUserImpressionPreview(mode) {
    const card = document.getElementById('uiPreviewCard');
    const content = document.getElementById('uiPreviewContent');
    const meta = document.getElementById('uiPreviewMeta');

    // 生成中的请求不自动 abort+重开。后端一旦开始调用供应商，前端 abort 不一定能取消扣费。
    if (_userImpressionGenerating) {
        showUserImpressionMsg('info', '用户画像正在生成中，请等待当前请求完成。');
        if (card) card.style.display = 'block';
        if (meta) meta.textContent = '生成中，请勿重复点击';
        return;
    }
    _userImpressionGenerating = true;
    const requestId = ++_userImpressionPreviewRequestId;
    const controller = new AbortController();
    _userImpressionPreviewAbortController = controller;
    _userImpressionPreview = null;

    if (card) card.style.display = 'block';
    if (content) content.innerHTML = '正在生成画像预览，可能需要一会儿...';
    if (meta) meta.textContent = (mode === 'update' ? '追加/更新模式' : '初始生成模式') + ' · 请求中，可点“取消预览”中断';
    try {
        const resp = await fetch('/api/user-impression/generate-preview', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({character_id:'default', mode: mode || 'initial'}),
            signal: controller.signal
        });
        const data = await resp.json();
        if (requestId !== _userImpressionPreviewRequestId || controller.signal.aborted) return;
        if (data.status !== 'ok') throw new Error(data.error || '生成失败');
        _userImpressionPreview = data;
        _userImpressionPreviewAbortController = null;
        _userImpressionGenerating = false;
        if (meta) {
            const ms = data.material_summary || {};
            meta.textContent = '模式：' + data.mode + ' · 记忆 ' + (ms.memory_count || 0) + ' 条 · 近期聊天 ' + (ms.recent_message_count || 0) + ' 条 · prompt ' + (ms.prompt_chars || 0) + ' 字';
        }
        if (content) content.innerHTML = renderUserImpressionObject(data.impression);
        showUserImpressionMsg('success', '画像预览已生成，确认后才会保存。');
    } catch (e) {
        if (requestId !== _userImpressionPreviewRequestId) return;
        if (e && e.name === 'AbortError') {
            if (content) content.innerHTML = '<div class="msg-box msg-info">画像生成已取消。</div>';
            if (meta) meta.textContent = '已取消';
            _userImpressionPreviewAbortController = null;
            _userImpressionGenerating = false;
            return;
        }
        _userImpressionPreviewAbortController = null;
        _userImpressionGenerating = false;
        if (content) content.innerHTML = '<div class="msg-box msg-error">生成失败：' + uiEsc(e.message) + '</div>';
    }
}

async function confirmUserImpressionPreview() {
    if (!_userImpressionPreview || !_userImpressionPreview.impression) {
        showUserImpressionMsg('error', '没有可保存的预览。');
        return;
    }
    try {
        const resp = await fetch('/api/user-impression/confirm', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({
                character_id: _userImpressionPreview.character_id || 'default',
                mode: _userImpressionPreview.mode || 'manual',
                source_message_count: _userImpressionPreview.source_message_count || 0,
                impression: _userImpressionPreview.impression
            })
        });
        const data = await resp.json();
        if (data.status !== 'ok') throw new Error(data.error || '保存失败');
        _userImpressionCurrent = data;
        closeUserImpressionPreview();
        renderUserImpression();
        showUserImpressionMsg('success', '画像已保存。');
    } catch (e) {
        showUserImpressionMsg('error', '保存失败：' + e.message);
    }
}

async function cancelUserImpressionGeneration() {
    // 只有仍在生成、并且本地仍持有飞行中的 fetch controller 时，才通知后端取消上游。
    if (!(_userImpressionGenerating && _userImpressionPreviewAbortController)) return false;
    try {
        await fetch('/api/user-impression/cancel', {
            method: 'POST',
            headers: {'Content-Type':'application/json'},
            body: JSON.stringify({character_id:'default'})
        });
    } catch (e) {
        console.warn('cancel user impression generation failed', e);
    }
    try { _userImpressionPreviewAbortController.abort(); } catch (e) {}
    _userImpressionPreviewAbortController = null;
    _userImpressionGenerating = false;
    return true;
}

function closeUserImpressionPreview() {
    _userImpressionPreview = null;
    const card = document.getElementById('uiPreviewCard');
    const content = document.getElementById('uiPreviewContent');
    const meta = document.getElementById('uiPreviewMeta');
    if (card) card.style.display = 'none';
    if (content) content.innerHTML = '';
    if (meta) meta.textContent = '';
}

async function clearUserImpressionPreview() {
    _userImpressionPreviewRequestId++;
    await cancelUserImpressionGeneration();
    closeUserImpressionPreview();
}

async function deleteUserImpression() {
    if (!confirm('确定删除当前用户画像吗？删除后可以重新生成。')) return;
    try {
        const resp = await fetch('/api/user-impression?character_id=default', {method:'DELETE'});
        const data = await resp.json();
        if (data.status !== 'ok') throw new Error(data.error || '删除失败');
        _userImpressionCurrent = null;
        clearUserImpressionPreview();
        renderUserImpression();
        showUserImpressionMsg('success', '画像已删除。');
    } catch (e) {
        showUserImpressionMsg('error', '删除失败：' + e.message);
    }
}

// ============================================================
// 聊天记录提取
// ============================================================

let _extractedMemories = [];
let _chatMemoryPalaceExtractRunning = false;

async function doExtractToMemoryPalaceFromChat() {
    if (_chatMemoryPalaceExtractRunning) return;
    _chatMemoryPalaceExtractRunning = true;
    const fileInput = document.getElementById('chatFile');
    const textInput = document.getElementById('chatInput');
    let text = '';
    if (fileInput && fileInput.files.length > 0) {
        text = await fileInput.files[0].text();
    } else if (textInput) {
        text = textInput.value;
    }
    if (!text.trim()) {
        showImportResult('error', '请输入或上传聊天记录');
        return;
    }
    const btn = document.getElementById('btn-extract-chat-palace');
    if (btn) { btn.disabled = true; btn.textContent = '提取中...'; }
    try {
        const resp = await fetch('/api/memory-palace/extract-text', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({text: text, preview: true})
        });
        const data = await resp.json();
        if (data.error || data.status === 'error') {
            showImportResult('error', '❌ ' + (data.error || '记忆宫殿提取失败'));
            return;
        }
        _extractedMemories = data.memories || data.nodes || [];
        const rawCount = data.raw_count ?? data.extracted ?? 0;
        const memoryCount = data.memory_count ?? _extractedMemories.length;
        if (_extractedMemories.length === 0) {
            const detail = data.message || (rawCount > 0
                ? ('模型返回了 ' + rawCount + ' 项，但没有项目符合记忆宫殿格式或包含 content 字段')
                : '模型没有返回可解析的记忆数组，或返回了空数组 []');
            showImportResult('info', detail);
            return;
        }
        renderExtractedMemories();
        document.getElementById('chat-extract-result').style.display = 'block';
        showImportResult('success', '✅ 已生成记忆宫殿预览：模型输出 ' + rawCount + ' 项，可导入 ' + memoryCount + ' 条，请勾选后确认导入到宫殿');
    } catch (e) {
        showImportResult('error', '❌ 记忆宫殿提取请求失败: ' + e.message);
    } finally {
        _chatMemoryPalaceExtractRunning = false;
        if (btn) { btn.disabled = false; btn.textContent = '提取到记忆宫殿'; }
    }
}

function renderExtractedMemories() {
    const list = document.getElementById('chat-extract-list');
    if (!list) return;
    list.innerHTML = _extractedMemories.map((m, i) => {
        const imp = m.importance !== undefined ? m.importance : '?';
        return '<label style="display:flex;align-items:flex-start;gap:8px;padding:8px 12px;'
            + 'border-radius:8px;margin-bottom:6px;background:#fafafa;border:1px solid #e5e7eb;cursor:pointer;">'
            + '<input type="checkbox" checked class="extract-check" value="' + i + '" style="margin-top:3px;">'
            + '<div style="flex:1;">'
            + '<div style="font-size:13px;color:#374151;">' + escapeHtml(m.content) + '</div>'
            + '<div style="font-size:11px;color:#9ca3af;margin-top:2px;">重要度: ' + imp + (m.room ? ' · 房间: ' + escapeHtml(m.room) : '') + (m.date ? ' · 日期: ' + escapeHtml(m.date) : '') + (m.pinned_until ? ' · 📌便利贴到 ' + escapeHtml(String(m.pinned_until).slice(0, 10)) : '') + '</div>'
            + '</div></label>';
    }).join('');
}


async function doImportExtractedToPalace() {
    const checked = [...document.querySelectorAll('.extract-check:checked')].map(c => parseInt(c.value));
    if (checked.length === 0) {
        showImportResult('error', '请至少选择一条记忆');
        return;
    }
    const memories = checked.map(i => _extractedMemories[i]).filter(Boolean);
    let imported = 0;
    let failed = 0;
    for (const m of memories) {
        try {
            const resp = await fetch('/api/memory-palace/nodes', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({
                    content: m.content || '',
                    room: m.room || 'living_room',
                    tags: Array.isArray(m.tags) ? m.tags.join('、') : (m.tags || ''),
                    importance: m.importance || 5,
                    mood: m.mood || 'neutral',
                    valence: m.valence ?? null,
                    arousal: m.arousal ?? null,
                    date: m.date || new Date().toISOString().slice(0, 10),
                    origin: 'extraction',
                    pinned_until: m.pinned_until || null,
                    metadata: {source: 'chat-extract-selection'}
                })
            });
            const data = await resp.json();
            if (data.error) failed++; else imported++;
        } catch (e) {
            failed++;
        }
    }
    showImportResult(failed ? 'info' : 'success', '已导入到记忆宫殿 ' + imported + ' 条' + (failed ? '，失败 ' + failed + ' 条' : ''));
    if (imported > 0) {
        const result = document.getElementById('chat-extract-result');
        const list = document.getElementById('chat-extract-list');
        if (result) result.style.display = 'none';
        if (list) list.innerHTML = '';
        _extractedMemories = [];
    }
}

function escapeHtml(str) {
    return str.replace(/&/g,'&').replace(/</g,'<').replace(/>/g,'>').replace(/"/g,'"');
}


try {
    window.previewMemoryPalaceFromSelectedConversations = previewMemoryPalaceFromSelectedConversations;
    window.renderConvMemoryPalacePreview = renderConvMemoryPalacePreview;
    window.toggleConvMemoryPreviewChecks = toggleConvMemoryPreviewChecks;
    window.closeConvMemoryPreview = closeConvMemoryPreview;
    window.importSelectedConvMemoryPreview = importSelectedConvMemoryPreview;
    window.openThreadMemoryModal = openThreadMemoryModal;
    
} catch (e) {

}


// ============================================
// 记忆宫殿备份导入
// ============================================
let _mpImportToken = '';

async function readMemoryPalaceImportText() {
    const file = document.getElementById('mpImportFile')?.files?.[0];
    if (file) return await file.text();
    return document.getElementById('mpImportInput')?.value?.trim() || '';
}

function mpImportTableLabel(t) {
    return {
        memory_palace_nodes: '记忆节点',
        memory_palace_vectors: '向量',
        memory_palace_links: '连接',
        memory_palace_event_boxes: '事件盒',
        memory_palace_extracted_messages: '已提取消息标记',
        memory_palace_extraction_cursor: '提取游标',
        memory_palace_state: '运行状态',
        memory_palace_recall_receipts: '召回回执'
    }[t] || t;
}

async function previewMemoryPalaceImport() {
    const box = document.getElementById('mpImportPreview');
    if (!box) return;
    try {
        const text = await readMemoryPalaceImportText();
        if (!text) { box.innerHTML = '<div style="color:var(--danger);">请选择文件或粘贴 JSON</div>'; return; }
        box.innerHTML = '正在解析...';
        const resp = await fetch('/api/memory-palace/import/preview', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({json:text})});
        const data = await resp.json();
        if (data.status === 'error') throw new Error(data.error || '预览失败');
        _mpImportToken = data.import_token || '';
        const counts = data.counts || {};
        const conflicts = data.conflicts || {};
        const tables = ['memory_palace_nodes','memory_palace_vectors','memory_palace_event_boxes','memory_palace_links','memory_palace_extracted_messages','memory_palace_extraction_cursor','memory_palace_state','memory_palace_recall_receipts'];
        let html = '<h3 style="margin:0 0 10px;">记忆宫殿备份预览</h3>';
        html += '<div style="font-size:13px;color:var(--text-muted);margin-bottom:10px;">schema: ' + escHtml(data.schema || '') + '</div>';
        html += '<div style="display:flex;gap:10px;flex-wrap:wrap;margin-bottom:12px;">';
        tables.forEach(t => { if ((counts[t] || 0) > 0) html += '<span style="padding:4px 8px;border-radius:999px;background:var(--bg-muted);">' + mpImportTableLabel(t) + ' ' + counts[t] + '</span>'; });
        html += '</div>';
        html += '<div style="font-size:13px;color:var(--text-muted);margin-bottom:12px;">已存在ID：' + (conflicts.existing_ids || 0) + ' · 完全重复内容：' + (conflicts.exact_duplicates || 0) + ' · 缺失连接引用：' + (conflicts.missing_link_refs || 0) + '</div>';
        html += '<div style="margin:12px 0;"><label class="form-label">导入策略</label><select id="mpImportStrategy" class="select-input"><option value="merge_skip_duplicates">合并导入，跳过冲突</option><option value="overwrite_ids">覆盖同 ID</option><option value="clear_restore">清空后恢复（危险）</option></select></div>';
        html += '<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:8px;margin:12px 0;">';
        tables.forEach(t => { const recommended = ['memory_palace_nodes','memory_palace_vectors','memory_palace_links','memory_palace_event_boxes'].includes(t); html += '<label><input type="checkbox" class="mp-import-include" value="' + t + '" ' + (recommended ? 'checked' : '') + '> ' + mpImportTableLabel(t) + '</label>'; });
        html += '</div>';
        if (data.sample_nodes && data.sample_nodes.length) {
            html += '<details style="margin-top:10px;"><summary>预览前 ' + data.sample_nodes.length + ' 条节点</summary><div style="margin-top:8px;max-height:220px;overflow:auto;">';
            data.sample_nodes.forEach(n => { html += '<div style="padding:6px 0;border-bottom:1px solid var(--border);"><b>' + escHtml(n.room || '') + '</b> ' + escHtml(n.content || '') + '</div>'; });
            html += '</div></details>';
        }
        html += '<div style="margin-top:14px;display:flex;gap:8px;"><button class="btn btn-primary" onclick="confirmMemoryPalaceImport()">确认导入</button></div>';
        box.innerHTML = html;
    } catch(e) { box.innerHTML = '<div style="color:var(--danger);">预览失败：' + escHtml(e.message) + '</div>'; }
}

async function confirmMemoryPalaceImport() {
    const box = document.getElementById('mpImportPreview');
    if (!_mpImportToken) { alert('请先预览'); return; }
    const strategy = document.getElementById('mpImportStrategy')?.value || 'merge_skip_duplicates';
    if (strategy === 'clear_restore' && !confirm('清空后恢复会删除当前记忆宫殿中所选表的数据。确定继续？')) return;
    const include = {};
    document.querySelectorAll('.mp-import-include').forEach(cb => include[cb.value] = cb.checked);
    try {
        if (box) box.innerHTML += '<div style="margin-top:10px;color:var(--text-muted);">正在导入...</div>';
        const resp = await fetch('/api/memory-palace/import/confirm', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({import_token:_mpImportToken, strategy, include})});
        const data = await resp.json();
        if (data.status === 'error') throw new Error(data.error || '导入失败');
        const imported = data.imported || {};
        let msg = '导入完成：' + (Object.keys(imported).map(k => mpImportTableLabel(k) + ' ' + imported[k]).join('，') || '无新增');
        if (box) box.innerHTML = '<div style="color:var(--success);">' + escHtml(msg) + '</div>';
        _mpImportToken = '';
    } catch(e) { if (box) box.innerHTML += '<div style="color:var(--danger);margin-top:8px;">导入失败：' + escHtml(e.message) + '</div>'; }
}
