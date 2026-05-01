/**
 * AI Memory Gateway - Dashboard JavaScript
 * 整合记忆管理、导入、导出功能
 */

// ============================================
// 全局状态
// ============================================
let allMemories = [];
let pendingJsonData = null;

// ============================================
// 初始化
// ============================================
document.addEventListener('DOMContentLoaded', () => {
    // 初始化侧边栏导航
    initNavigation();
    // 初始化Tab切换
    initTabs();
    // 加载记忆数据
    loadMemories();
    // 加载导出统计
    loadExportStats();
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
    }
    if (name === 'threads') {
        loadThreads();
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
// 记忆管理功能
// ============================================
async function loadMemories() {
    try {
        const resp = await fetch('/api/memories');
        const data = await resp.json();
        allMemories = data.memories || [];
        document.getElementById('stats').textContent = '共 ' + allMemories.length + ' 条记忆';
        filterAndSort();
    } catch(e) {
        showManageMsg('error', '加载失败：' + e.message);
    }
}

function renderTable(mems) {
    const tbody = document.getElementById('tbody');
    tbody.innerHTML = mems.map(m => 
        '<tr data-id="' + m.id + '">' +
        '<td class="col-check"><input type="checkbox" class="mem-check" value="' + m.id + '"></td>' +
        '<td class="col-id">' + m.id + '</td>' +
        '<td class="col-content"><textarea class="content-textarea" id="c_' + m.id + '">' + escHtml(m.content) + '</textarea></td>' +
        '<td class="col-importance"><input type="number" class="importance-input" id="i_' + m.id + '" value="' + m.importance + '" min="1" max="10"></td>' +
        '<td class="col-source">' + (m.source_session || '-') + '</td>' +
        '<td class="col-time">' + fmtTime(m.created_at) + '</td>' +
        '<td class="col-actions"><div class="row-actions">' +
            '<button class="btn btn-primary btn-sm" onclick="saveMem(' + m.id + ')">保存</button>' +
            '<button class="btn btn-danger btn-sm" onclick="delMem(' + m.id + ')">删除</button>' +
        '</div></td>' +
        '</tr>'
    ).join('');
}

function escHtml(s) {
    return s
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
}

function fmtTime(s) {
    if (!s) return '-';
    return s;
}

function filterAndSort() {
    const q = document.getElementById('searchBox').value.trim().toLowerCase();
    const sort = document.getElementById('sortSelect').value;
    const dateVal = document.getElementById('dateFilter').value;
    
    let mems = allMemories;
    
    // 关键词筛选
    if (q) {
        mems = mems.filter(m => m.content.toLowerCase().includes(q));
    }
    
    // 日期筛选
    if (dateVal) {
        mems = mems.filter(m => m.created_at && fmtTime(m.created_at).slice(0, 10) === dateVal);
    }
    
    // 排序
    mems = [...mems].sort((a, b) => {
        if (sort === 'id-desc') return b.id - a.id;
        if (sort === 'id-asc') return a.id - b.id;
        if (sort === 'imp-desc') return b.importance - a.importance || b.id - a.id;
        if (sort === 'imp-asc') return a.importance - b.importance || a.id - b.id;
        return 0;
    });
    
    renderTable(mems);
    
    // 更新统计
    const parts = [];
    if (q || dateVal) {
        parts.push('筛选到 ' + mems.length + ' / ' + allMemories.length + ' 条');
        if (dateVal) parts.push('日期: ' + dateVal);
    } else {
        parts.push('共 ' + allMemories.length + ' 条记忆');
    }
    document.getElementById('stats').textContent = parts.join('  ');
}

function clearDateFilter() {
    document.getElementById('dateFilter').value = '';
    filterAndSort();
}

async function saveMem(id) {
    const content = document.getElementById('c_' + id).value;
    const importance = parseInt(document.getElementById('i_' + id).value);
    
    try {
        const resp = await fetch('/api/memories/' + id, {
            method: 'PUT',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({content, importance})
        });
        const data = await resp.json();
        if (data.error) {
            showManageMsg('error', '❌ ' + data.error);
        } else {
            showManageMsg('success', '✅ 已保存 #' + id);
            loadMemories();
        }
    } catch(e) {
        showManageMsg('error', '❌ ' + e.message);
    }
}

async function delMem(id) {
    if (!confirm('确定删除 #' + id + '？此操作不可撤销。')) return;
    
    try {
        const resp = await fetch('/api/memories/' + id, { method: 'DELETE' });
        const data = await resp.json();
        if (data.error) {
            showManageMsg('error', '❌ ' + data.error);
        } else {
            showManageMsg('success', '✅ 已删除 #' + id);
            loadMemories();
        }
    } catch(e) {
        showManageMsg('error', '❌ ' + e.message);
    }
}

async function batchSave() {
    const rows = document.querySelectorAll('#tbody tr');
    if (rows.length === 0) {
        showManageMsg('error', '没有记忆可保存');
        return;
    }
    
    const updates = [];
    rows.forEach(row => {
        const id = parseInt(row.dataset.id);
        const cEl = document.getElementById('c_' + id);
        const iEl = document.getElementById('i_' + id);
        if (cEl && iEl) {
            updates.push({
                id,
                content: cEl.value,
                importance: parseInt(iEl.value)
            });
        }
    });
    
    if (!confirm('确定保存全部 ' + updates.length + ' 条记忆的修改？')) return;
    
    try {
        const resp = await fetch('/api/memories/batch-update', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({updates: updates})
        });
        const data = await resp.json();
        if (data.error) {
            showManageMsg('error', '❌ ' + data.error);
        } else {
            showManageMsg('success', '✅ 已保存 ' + data.updated + ' 条');
            loadMemories();
        }
    } catch(e) {
        showManageMsg('error', '❌ ' + e.message);
    }
}

async function batchDelete() {
    const checked = [...document.querySelectorAll('.mem-check:checked')].map(c => parseInt(c.value));
    
    if (checked.length === 0) {
        showManageMsg('error', '请先勾选要删除的记忆');
        return;
    }
    
    if (!confirm('确定删除选中的 ' + checked.length + ' 条记忆？此操作不可撤销。')) return;
    
    try {
        const resp = await fetch('/api/memories/batch-delete', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({ids: checked})
        });
        const data = await resp.json();
        if (data.error) {
            showManageMsg('error', '❌ ' + data.error);
        } else {
            showManageMsg('success', '✅ 已删除 ' + data.deleted + ' 条');
            loadMemories();
        }
    } catch(e) {
        showManageMsg('error', '❌ ' + e.message);
    }
}

function toggleAll() {
    const val = event.target.checked;
    document.querySelectorAll('.mem-check').forEach(c => c.checked = val);
    document.getElementById('selectAll').checked = val;
    document.getElementById('selectAllHead').checked = val;
}

function showManageMsg(type, text) {
    const container = document.getElementById('manage-msg');
    container.innerHTML = '<div class="msg msg-' + type + '">' + text + '</div>';
    setTimeout(() => {
        container.innerHTML = '';
    }, 4000);
}

// ============================================
// 导入功能
// ============================================
async function doTextImport() {
    const file = document.getElementById('txtFile').files[0];
    const text = document.getElementById('txtInput').value.trim();
    const skip = document.getElementById('skipScore').checked;
    
    let content = '';
    if (file) {
        content = await file.text();
    } else if (text) {
        content = text;
    } else {
        showImportResult('error', '请先上传文件或输入文本');
        return;
    }
    
    const lines = content.split('\n').map(l => l.trim()).filter(l => l.length > 0);
    if (lines.length === 0) {
        showImportResult('error', '没有找到有效的记忆条目');
        return;
    }
    
    showImportResult('info', skip 
        ? '正在导入 ' + lines.length + ' 条记忆...' 
        : '正在为 ' + lines.length + ' 条记忆自动评分，请稍候...');
    
    try {
        const resp = await fetch('/import/text', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({lines: lines, skip_scoring: skip})
        });
        const data = await resp.json();
        if (data.error) {
            showImportResult('error', '❌ ' + data.error);
        } else {
            showImportResult('success', '✅ 导入完成！新增 ' + data.imported + ' 条，跳过 ' + data.skipped + ' 条（已存在），总计 ' + data.total + ' 条');
            // 刷新记忆列表
            loadMemories();
        }
    } catch(e) {
        showImportResult('error', '❌ 请求失败：' + e.message);
    }
}

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
        const mems = parsed.memories || [];
        if (mems.length === 0) {
            showImportResult('error', '❌ 没有找到 memories 字段，请确认这是从导出功能导出的文件');
            preview.innerHTML = '';
            return;
        }
        
        pendingJsonData = parsed;
        let html = '<p><b>预览：共 ' + mems.length + ' 条记忆</b></p>';
        const show = mems.slice(0, 10);
        show.forEach(m => {
            html += '<div class="preview-item">权重 ' + (m.importance || '?') + ' | ' + (m.content || '').substring(0, 80) + '</div>';
        });
        if (mems.length > 10) {
            html += '<div class="preview-item" style="color:#999;">...还有 ' + (mems.length - 10) + ' 条</div>';
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
        const resp = await fetch('/import/memories', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(pendingJsonData)
        });
        const data = await resp.json();
        if (data.error) {
            showImportResult('error', '❌ ' + data.error);
        } else {
            showImportResult('success', '✅ 导入完成！新增 ' + data.imported + ' 条，跳过 ' + data.skipped + ' 条（已存在），总计 ' + data.total + ' 条');
            loadMemories();
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
    try {
        const resp = await fetch('/api/memories');
        const data = await resp.json();
        const count = (data.memories || []).length;
        el.textContent = '当前共有 ' + count + ' 条记忆';
    } catch(e) {
        el.textContent = '无法加载统计';
    }
}

function doExport() {
    // 直接跳转到导出接口，浏览器会下载文件
    window.location.href = '/export/memories';
}


// ============================================
// 对话记录管理
// ============================================
let convCurrentPage = 1;

async function loadConversationList(page) {
    convCurrentPage = page || 1;
    const container = document.getElementById('conv-list-container');
    try {
        const resp = await fetch(`/api/conversations?page=${convCurrentPage}&per_page=20`);
        const data = await resp.json();
        if (data.error) { container.innerHTML = `<div style="color:red;">${data.error}</div>`; return; }
        
        document.getElementById('conv-list-count').textContent = `共 ${data.total} 个对话`;
        renderConvList(data.conversations || []);
        renderConvPagination(data.total, data.per_page);
    } catch(e) {
        container.innerHTML = `<div style="color:red;">加载失败: ${e.message}</div>`;
    }
}

function renderConvList(conversations) {
    const container = document.getElementById('conv-list-container');
    if (!conversations.length) {
        container.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:20px 0;">暂无对话</div>';
        return;
    }
    
    let html = `<div style="display:flex;gap:8px;align-items:center;padding:8px 0;border-bottom:1px solid var(--border);margin-bottom:4px;">
        <label style="display:flex;align-items:center;gap:4px;cursor:pointer;font-size:13px;">
            <input type="checkbox" id="conv-select-all" onchange="toggleConvSelectAll(this.checked)"> 全选
        </label>
        <button class="btn btn-sm" onclick="batchDeleteConversations()" id="conv-batch-delete-btn" style="display:none;font-size:12px;">🗑️ 批量删除</button>
        <button class="btn btn-sm" onclick="batchMergeSessions()" id="conv-batch-merge-btn" style="display:none;font-size:12px;">🔗 合并到...</button>
        <span id="conv-selected-count" style="color:var(--text-muted);font-size:12px;display:none;"></span>
    </div>`;
    
    for (const conv of conversations) {
        const msgCount = conv.message_count || '';
        const totalTokens = conv.total_tokens || 0;
        const tokenStr = totalTokens > 0 ? (totalTokens >= 1000 ? (totalTokens / 1000).toFixed(1) + 'K' : totalTokens) : '';
        const lastTime = conv.last_time || '';
        const timeStr = lastTime ? formatConvTime(lastTime) : '';
        
        html += `<div style="display:flex;align-items:center;padding:10px 8px;border-bottom:1px solid var(--border);cursor:pointer;" onclick="loadConvDetail('${conv.session_id}')">
            <input type="checkbox" class="conv-checkbox" value="${conv.session_id}" onclick="event.stopPropagation();updateConvSelectionCount()" style="margin-right:10px;">
            <div style="flex:1;min-width:0;">
                <div style="font-weight:500;font-size:14px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${conv.title}</div>
                <div style="color:var(--text-muted);font-size:12px;margin-top:2px;white-space:nowrap;overflow:hidden;text-overflow:ellipsis;">${conv.preview || ''}</div>
            </div>
            <div style="text-align:right;flex-shrink:0;margin-left:12px;">
                <div style="color:var(--text-muted);font-size:12px;">${timeStr}</div>
                ${msgCount ? `<div style="color:var(--text-muted);font-size:12px;margin-top:2px;">${msgCount} 条</div>` : ''}
                ${tokenStr ? `<div style="color:var(--text-muted);font-size:11px;margin-top:2px;">🪙 ${tokenStr}</div>` : ''}
            </div>
        </div>`;
    }
    container.innerHTML = html;
}

function renderConvPagination(total, perPage) {
    const pages = Math.ceil(total / perPage);
    const el = document.getElementById('conv-pagination');
    if (pages <= 1) { el.innerHTML = ''; return; }
    let html = '';
    if (convCurrentPage > 1) html += `<button class="btn btn-sm" onclick="loadConversationList(${convCurrentPage-1})">上一页</button>`;
    html += `<span style="font-size:13px;">第 ${convCurrentPage} / ${pages} 页</span>`;
    if (convCurrentPage < pages) html += `<button class="btn btn-sm" onclick="loadConversationList(${convCurrentPage+1})">下一页</button>`;
    el.innerHTML = html;
}

function toggleConvSelectAll(checked) {
    document.querySelectorAll('.conv-checkbox').forEach(cb => cb.checked = checked);
    updateConvSelectionCount();
}

function updateConvSelectionCount() {
    const checked = document.querySelectorAll('.conv-checkbox:checked');
    const countEl = document.getElementById('conv-selected-count');
    const btnEl = document.getElementById('conv-batch-delete-btn');
    const mergeBtn = document.getElementById('conv-batch-merge-btn');
    if (checked.length > 0) {
        countEl.style.display = ''; countEl.textContent = `已选 ${checked.length} 个`;
        btnEl.style.display = ''; if (mergeBtn) mergeBtn.style.display = '';
    } else {
        countEl.style.display = 'none'; btnEl.style.display = 'none'; if (mergeBtn) mergeBtn.style.display = 'none';
    }
}

async function batchDeleteConversations() {
    const ids = Array.from(document.querySelectorAll('.conv-checkbox:checked')).map(cb => cb.value);
    if (!ids.length || !confirm(`确定删除选中的 ${ids.length} 个对话？此操作不可撤销。`)) return;
    try {
        await fetch('/api/conversations/batch-delete', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({session_ids: ids}) });
        loadConversationList(convCurrentPage);
    } catch(e) { alert('删除失败: ' + e.message); }
}

async function batchMergeSessions() {
    const ids = Array.from(document.querySelectorAll('.conv-checkbox:checked')).map(cb => cb.value);
    if (!ids.length) return;
    const targetId = prompt('输入目标 Session ID:', '');
    if (!targetId || !confirm(`确定将 ${ids.length} 个对话合并到「${targetId}」？不可撤销。`)) return;
    try {
        const resp = await fetch('/api/admin/merge-sessions', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({source_ids: ids, target_id: targetId}) });
        const data = await resp.json();
        if (data.error) { alert('合并失败: ' + data.error); return; }
        alert(`合并完成！${data.merged_messages} 条消息 → ${targetId}`);
        loadConversationList(convCurrentPage);
    } catch(e) { alert('请求失败: ' + e.message); }
}

async function loadConvDetail(sessionId) {
    const panel = document.getElementById('conv-detail-panel');
    const msgEl = document.getElementById('conv-detail-messages');
    document.getElementById('conv-detail-title').textContent = `对话详情: ${sessionId}`;
    panel.style.display = '';
    msgEl.innerHTML = '<div style="text-align:center;padding:20px;">加载中...</div>';
    try {
        const resp = await fetch(`/api/conversations/${sessionId}/messages?limit=200`);
        const data = await resp.json();
        if (!data.messages || !data.messages.length) { msgEl.innerHTML = '暂无消息'; return; }
        let html = '';
        for (const m of data.messages) {
            const isUser = m.role === 'user';
            html += `<div style="margin-bottom:12px;padding:10px;border-radius:8px;background:${isUser ? 'var(--bg-card)' : 'transparent'};border:1px solid var(--border);">
                <div style="font-weight:600;font-size:12px;color:${isUser ? 'var(--primary)' : 'var(--text-muted)'};">${isUser ? 'User' : 'Assistant'}</div>
                <div style="margin-top:4px;font-size:14px;white-space:pre-wrap;">${(m.content||'').substring(0,2000)}</div>
            </div>`;
        }
        msgEl.innerHTML = html;
    } catch(e) { msgEl.innerHTML = `<div style="color:red;">加载失败: ${e.message}</div>`; }
}

function closeConvDetail() {
    document.getElementById('conv-detail-panel').style.display = 'none';
}

function formatConvTime(isoStr) {
    try {
        const d = new Date(isoStr);
        const now = new Date();
        const diffMs = now - d;
        const diffMins = Math.floor(diffMs / 60000);
        const diffHours = Math.floor(diffMs / 3600000);
        const diffDays = Math.floor(diffMs / 86400000);
        if (diffMins < 1) return '刚刚';
        if (diffMins < 60) return diffMins + '分钟前';
        if (diffHours < 24) return diffHours + '小时前';
        if (diffDays < 7) return diffDays + '天前';
        return d.toLocaleDateString('zh-CN', { month: 'short', day: 'numeric' });
    } catch(e) { return ''; }
}


// ============================================
// 对话线管理
// ============================================
let _threadData = { threads: [], active_session_id: '' };
let _summaryEditSid = '';

async function loadThreads() {
    try {
        const [statusResp, threadsResp] = await Promise.all([fetch('/api/partition/status'), fetch('/api/partition/threads')]);
        const status = await statusResp.json();
        const data = await threadsResp.json();
        _threadData = data;
        renderThreadStatus(status);
        renderThreadList(data.threads);
        updateCopyFromSelect(data.threads);
    } catch(e) {
        document.getElementById('thread-status').textContent = '加载失败: ' + e.message;
    }
}

function renderThreadStatus(status) {
    const el = document.getElementById('thread-status');
    if (!status.enabled) {
        el.innerHTML = '<span style="color:var(--danger);">⚠️ 分区缓存未启用（CACHE_PARTITION_ENABLED=false）</span>';
        return;
    }
    el.innerHTML = `<div style="display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px;">
        <div><strong>活跃对话线</strong><br><span style="font-size:18px;color:var(--primary);">${status.active_session_id || '未设置'}</span></div>
        <div><strong>轮转周期</strong><br>每 ${status.partition_x} 轮</div>
        <div><strong>摘要长度</strong><br>${status.summary_length} 字</div>
        <div><strong>A区起始轮</strong><br>第 ${status.a_start_round} 轮</div>
    </div>`;
}

function renderThreadList(threads) {
    const el = document.getElementById('thread-list');
    if (!threads || !threads.length) { el.innerHTML = '<div style="text-align:center;color:var(--text-muted);padding:20px 0;">暂无对话线</div>'; return; }
    let html = '';
    for (const t of threads) {
        const isActive = t.is_active;
        const tokens = t.chat_tokens > 0 ? (t.chat_tokens >= 1000 ? (t.chat_tokens / 1000).toFixed(1) + 'K' : t.chat_tokens) : '0';
        const summaryPreview = t.summary ? (t.summary.substring(0, 80) + (t.summary.length > 80 ? '...' : '')) : '（无摘要）';
        const updatedStr = t.updated_at ? formatConvTime(t.updated_at) : '';
        html += `<div style="border:1px solid ${isActive ? 'var(--primary)' : 'var(--border)'};border-radius:8px;padding:14px;margin-bottom:8px;${isActive ? 'box-shadow:0 0 0 1px var(--primary);' : ''}">
            <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px;">
                <div style="display:flex;align-items:center;gap:8px;">
                    <span style="font-weight:600;font-size:15px;">${t.session_id}</span>
                    ${isActive ? '<span style="background:var(--primary);color:white;font-size:11px;padding:2px 8px;border-radius:10px;">活跃</span>' : ''}
                </div>
                <div style="display:flex;gap:6px;">
                    <button class="btn btn-sm" onclick="openSummaryModal('${t.session_id}')">📝 摘要</button>
                    ${!isActive ? `<button class="btn btn-sm btn-primary" onclick="switchThread('${t.session_id}')">切换到此</button>` : ''}
                </div>
            </div>
            <div style="color:var(--text-muted);font-size:13px;line-height:1.5;">
                <div>${summaryPreview}</div>
                <div style="margin-top:6px;display:flex;gap:16px;">
                    <span>💬 ${t.message_count} 条</span><span>🪙 ${tokens}</span><span>📝 摘要 ${t.summary_length} 字</span>
                    ${updatedStr ? `<span>更新于 ${updatedStr}</span>` : ''}
                </div>
            </div>
        </div>`;
    }
    el.innerHTML = html;
}

function updateCopyFromSelect(threads) {
    const sel = document.getElementById('new-thread-copy-from');
    sel.innerHTML = '<option value="">不继承，从零开始</option>';
    for (const t of threads) {
        if (t.summary_length > 0) sel.innerHTML += `<option value="${t.session_id}">${t.session_id} (${t.summary_length}字)</option>`;
    }
}

async function createThread() {
    const newId = document.getElementById('new-thread-id').value.trim();
    const copyFrom = document.getElementById('new-thread-copy-from').value;
    const msgEl = document.getElementById('thread-create-msg');
    if (!newId) { msgEl.innerHTML = '<div style="color:red;">请输入对话线ID</div>'; return; }
    try {
        const resp = await fetch('/api/partition/thread', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({session_id: newId, copy_summary_from: copyFrom}) });
        const data = await resp.json();
        if (data.error) { msgEl.innerHTML = `<div style="color:red;">${data.error}</div>`; return; }
        msgEl.innerHTML = `<div style="color:green;">✅ 创建成功${data.summary_length > 0 ? '（继承了' + data.summary_length + '字摘要）' : ''}</div>`;
        document.getElementById('new-thread-id').value = '';
        loadThreads();
    } catch(e) { msgEl.innerHTML = `<div style="color:red;">请求失败: ${e.message}</div>`; }
}

async function switchThread(sessionId) {
    if (!confirm(`确定切换到对话线「${sessionId}」吗？`)) return;
    try {
        const resp = await fetch('/api/partition/switch', { method: 'POST', headers: {'Content-Type':'application/json'}, body: JSON.stringify({session_id: sessionId}) });
        const data = await resp.json();
        if (data.error) { alert('切换失败: ' + data.error); return; }
        loadThreads();
    } catch(e) { alert('请求失败: ' + e.message); }
}

async function openSummaryModal(sessionId) {
    _summaryEditSid = sessionId;
    document.getElementById('summary-modal-sid').textContent = sessionId;
    const thread = _threadData.threads.find(t => t.session_id === sessionId);
    document.getElementById('summary-editor').value = thread ? (thread.summary || '') : '';
    updateSummaryCharCount();
    document.getElementById('summaryModal').style.display = 'flex';
}

function closeSummaryModal() { document.getElementById('summaryModal').style.display = 'none'; _summaryEditSid = ''; }

function updateSummaryCharCount() {
    document.getElementById('summary-char-count').textContent = document.getElementById('summary-editor').value.length + ' 字';
}

document.addEventListener('DOMContentLoaded', () => {
    const editor = document.getElementById('summary-editor');
    if (editor) editor.addEventListener('input', updateSummaryCharCount);
});

async function saveSummary() {
    try {
        const resp = await fetch('/api/partition/summary', { method: 'PUT', headers: {'Content-Type':'application/json'}, body: JSON.stringify({session_id: _summaryEditSid, summary: document.getElementById('summary-editor').value}) });
        const data = await resp.json();
        if (data.error) { alert('保存失败: ' + data.error); return; }
        closeSummaryModal(); loadThreads();
    } catch(e) { alert('请求失败: ' + e.message); }
}

async function clearSummary() {
    if (!confirm(`确定清空「${_summaryEditSid}」的摘要吗？`)) return;
    try {
        const resp = await fetch('/api/partition/summary', { method: 'DELETE', headers: {'Content-Type':'application/json'}, body: JSON.stringify({session_id: _summaryEditSid}) });
        const data = await resp.json();
        if (data.error) { alert('清空失败: ' + data.error); return; }
        closeSummaryModal(); loadThreads();
    } catch(e) { alert('请求失败: ' + e.message); }
}
