let _mpRooms = [];
let _mpNodes = [];
let _mpCurrentRoom = '';
let _mpEditingId = null;
let _mpEventBoxes = [];
let _mpCurrentEventBoxId = null;
let _mpShowNodeIds = false;

function mpEsc(s) {
    return String(s == null ? '' : s)
        .replace(/&/g, '&')
        .replace(/</g, '<')
        .replace(/>/g, '>')
        .replace(/"/g, '"')
        .replace(/'/g, '&#39;');
}

function mpMsg(text, type) {
    const el = document.getElementById('mp-msg');
    if (!el) return;
    if (!text) {
        el.innerHTML = '';
        return;
    }
    const color = type === 'error' ? '#dc2626' : '#059669';
    el.innerHTML = '<div style=\"padding:10px 12px;border-radius:8px;background:#fff;border:1px solid ' + color + ';color:' + color + ';margin-bottom:12px;\">' + mpEsc(text) + '</div>';
}

function mpRoomMeta(roomId) {
    return _mpRooms.find(r => r.room === roomId) || {room: roomId, label: roomId || '全部房间', description: '', color: '#64748b', count: 0};
}

function mpDateTimeLocalValue(value) {
    if (!value) return '';
    const text = String(value);
    if (/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}/.test(text)) return text.slice(0, 16);
    const d = new Date(text);
    if (Number.isNaN(d.getTime())) return '';
    const pad = n => String(n).padStart(2, '0');
    return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate()) + 'T' + pad(d.getHours()) + ':' + pad(d.getMinutes());
}

function mpPinnedText(value) {
    if (!value) return '';
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return '';
    const diff = d.getTime() - Date.now();
    if (diff <= 0) return '📌 已过期';
    const days = Math.max(1, Math.ceil(diff / 86400000));
    return '📌 便利贴剩余 ' + days + ' 天';
}

async function loadMemoryPalace() {
    const root = document.getElementById('section-memory-palace');
    if (!root) return;

    try {
        mpMsg('');
        clearDigestPreviewArea();
        const resp = await fetch('/api/memory-palace/rooms');
        const data = await resp.json();
        if (data.error) throw new Error(data.error);
        _mpRooms = data.rooms || [];
        renderMemoryPalaceRooms();
        await loadMemoryPalaceNodes(_mpCurrentRoom);
        await loadMemoryPalaceEventBoxes();
    } catch (e) {
        mpMsg('加载记忆宫殿失败：' + e.message, 'error');
        const roomsEl = document.getElementById('mpRooms');
        if (roomsEl) roomsEl.innerHTML = '<div style=\"color:var(--text-muted);\">加载失败</div>';
    }
}

function renderMemoryPalaceRooms() {
    const el = document.getElementById('mpRooms');
    if (!el) return;
    const total = _mpRooms.reduce((sum, r) => sum + Number(r.count || 0), 0);
    const cards = [];

    cards.push(mpRoomCardHtml({
        room: '',
        label: '全部房间',
        description: '查看所有记忆节点',
        color: '#334155',
        count: total
    }));

    _mpRooms.forEach(room => cards.push(mpRoomCardHtml(room)));
    el.innerHTML = cards.join('');
    updateMemoryPalaceRoomTitle();
}

function mpRoomCardHtml(room) {
    const active = (_mpCurrentRoom || '') === (room.room || '');
    const color = room.color || '#64748b';
    const roomId = room.room || '';
    return '<div class=\"mp-room-card\" data-room=\"' + mpEsc(roomId) + '\" ' +
        'style=\"cursor:pointer;min-height:112px;border-radius:18px;padding:16px;border:2px solid ' + (active ? color : 'transparent') + ';background:linear-gradient(135deg,' + color + '22,#ffffff);box-shadow:0 4px 14px rgba(15,23,42,0.08);\">' +
        '<div style=\"display:flex;justify-content:space-between;align-items:flex-start;gap:8px;\">' +
            '<div style=\"font-size:18px;font-weight:800;color:' + color + ';\">' + mpEsc(room.label) + '</div>' +
            '<div style=\"min-width:32px;height:32px;border-radius:999px;background:' + color + ';color:white;display:flex;align-items:center;justify-content:center;font-weight:700;\">' + Number(room.count || 0) + '</div>' +
        '</div>' +
        '<div style=\"margin-top:10px;color:var(--text-muted);font-size:13px;line-height:1.45;\">' + mpEsc(room.description || '') + '</div>' +
    '</div>';
}

async function selectMemoryPalaceRoom(room) {
    _mpCurrentRoom = room || '';
    closeMemoryPalaceEditor();
    renderMemoryPalaceRooms();
    await loadMemoryPalaceNodes(_mpCurrentRoom);
}

function updateMemoryPalaceRoomTitle() {
    const el = document.getElementById('mpCurrentRoomTitle');
    if (!el) return;
    const room = mpRoomMeta(_mpCurrentRoom);
    el.textContent = _mpCurrentRoom ? (room.label + ' · ' + room.description) : '全部房间';
}

async function loadMemoryPalaceNodes(room) {
    const el = document.getElementById('mpNodeList');
    if (el) el.innerHTML = '<div style=\"color:var(--text-muted);padding:16px;\">加载中...</div>';

    const params = new URLSearchParams();
    params.set('limit', '200');
    params.set('archived', 'false');
    if (room) params.set('room', room);

    try {
        const resp = await fetch('/api/memory-palace/nodes?' + params.toString());
        const data = await resp.json();
        if (data.error) throw new Error(data.error);
        _mpNodes = data.nodes || [];
        renderMemoryPalaceNodes();
        updateMemoryPalaceRoomTitle();
    } catch (e) {
        mpMsg('加载记忆节点失败：' + e.message, 'error');
        if (el) el.innerHTML = '<div style=\"color:var(--text-muted);padding:16px;\">加载失败</div>';
    }
}

function renderMemoryPalaceNodes() {
    const el = document.getElementById('mpNodeList');
    if (!el) return;
    if (!_mpNodes.length) {
        el.innerHTML = '<div class=\"card\" style=\"padding:24px;color:var(--text-muted);text-align:center;\">这个房间还没有记忆。可以点“新增记忆”手动放一条进去。</div>';
        return;
    }
    el.innerHTML = _mpNodes.map(node => {
        const room = mpRoomMeta(node.room);
        const color = room.color || '#64748b';
        const tags = node.tags ? String(node.tags).split(/[、,，\\n]/).map(t => t.trim()).filter(Boolean) : [];
        const pinnedText = mpPinnedText(node.pinned_until);
        const nodeSeq = String(_mpNodes.indexOf(node) + 1).padStart(2, '0');
        const idBadge = _mpShowNodeIds ? ' <span title=\"' + mpEsc(node.id || '') + '\" style=\"font-weight:900;color:#e75480;margin-left:8px;font-size:20px;vertical-align:middle;\">#' + nodeSeq + '</span>' : '';
        return '<div class=\"card mp-node-card\" data-id=\"' + mpEsc(node.id) + '\" style=\"padding:16px;border-top:4px solid ' + color + ';\">' +
            '<div style=\"display:flex;justify-content:space-between;gap:10px;margin-bottom:10px;\">' +
                '<div>' +
                    '<div style=\"font-weight:800;color:' + color + ';\">' + mpEsc(room.label || node.room) + idBadge + '</div>' +
                    '<div style=\"font-size:12px;color:var(--text-muted);margin-top:2px;\">importance ' + mpEsc(node.importance || 5) + ' · ' + mpEsc(node.mood || 'neutral') + (pinnedText ? ' · ' + mpEsc(pinnedText) : '') + '</div>' +
                '</div>' +
                '<div style=\"display:flex;gap:6px;align-items:flex-start;\">' +
                    '<button class=\"mp-add-node-current-box\" data-id=\"' + mpEsc(node.id) + '\" style=\"padding:4px 8px;border:1px solid var(--border-color);border-radius:6px;background:#fff;cursor:pointer;font-size:12px;\">入盒</button>' +
                    '<button class=\"mp-manual-bind-node\" data-id=\"' + mpEsc(node.id) + '\" style=\"padding:4px 8px;border:1px solid var(--border-color);border-radius:6px;background:#fff;cursor:pointer;font-size:12px;\">绑定</button>' +
                    '<button class=\"mp-edit-node\" data-id=\"' + mpEsc(node.id) + '\" style=\"padding:4px 8px;border:1px solid var(--border-color);border-radius:6px;background:#fff;cursor:pointer;font-size:12px;\">编辑</button>' +
                    '<button class=\"mp-delete-node\" data-id=\"' + mpEsc(node.id) + '\" style=\"padding:4px 8px;border:1px solid #dc2626;color:#dc2626;border-radius:6px;background:#fff;cursor:pointer;font-size:12px;\">删除</button>' +
                '</div>' +
            '</div>' +
            '<div style=\"white-space:pre-wrap;line-height:1.65;font-size:14px;\">' + mpEsc(node.content || '') + '</div>' +
            (tags.length ? '<div style=\"margin-top:12px;display:flex;gap:6px;flex-wrap:wrap;\">' + tags.map(t => '<span style=\"font-size:12px;padding:3px 8px;border-radius:999px;background:' + color + '18;color:' + color + ';\">#' + mpEsc(t) + '</span>').join('') + '</div>' : '') +
            '<div style=\"margin-top:10px;font-size:12px;color:var(--text-muted);\">记忆日期：' + mpEsc(node.date || (node.created_at || '').slice(0, 10)) + ' · 入库：' + mpEsc((node.created_at || '').slice(0, 19).replace('T', ' ')) + (node.access_count ? ' · 访问 ' + node.access_count : '') + '</div>' +
            '<div class=\"mp-inline-editor\" data-id=\"' + mpEsc(node.id) + '\" style=\"display:none;margin-top:14px;padding-top:14px;border-top:1px dashed var(--border-color);\"></div>' +
        '</div>';
    }).join('');
}


let _digestPreviewActions = [];

let _digestRunning = false;
async function runCognitiveDigestion() {
    if (_digestRunning) return;
    _digestRunning = true;
    try {
        mpMsg('认知消化预览中（需要调用 LLM，可能较慢）...');
        const resp = await fetch('/api/memory-palace/digest/preview', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({})});
        const data = await resp.json();
        if (data.status === 'error') throw new Error(data.error || '消化失败');
        if (data.status === 'empty') { mpMsg('没有待消化的内容'); return; }
        if (data.status === 'no_actions') { mpMsg(data.message || 'LLM 未返回需要执行的动作'); return; }
        if (data.status === 'parse_empty') { mpMsg((data.message || 'LLM 返回了内容，但没有解析出有效动作') + (data.raw_preview ? '：' + data.raw_preview.substring(0, 160) : ''), 'error'); return; }
        _digestPreviewActions = data.actions || [];
        mpMsg('');
        if (_digestPreviewActions.length === 0) { mpMsg('LLM 未返回有效动作'); return; }
        renderDigestPreview(_digestPreviewActions);
    } catch (e) { mpMsg('认知消化预览失败：' + e.message, 'error'); } finally { _digestRunning = false; }
}

function clearDigestPreviewArea() {
    const preview = document.getElementById('mpDigestPreview');
    if (preview) { preview.style.display = 'none'; preview.innerHTML = ''; }
}

function renderDigestPreview(actions) {
    const el = document.getElementById('mpDigestPreview') || document.getElementById('mpNodeList');
    if (!el) return;
    el.style.display = 'block';
    const ACTION_LABELS = {
        resolve: '化解 -> 卧室', deepen: '加深创伤', fade: '淡忘',
        fulfill: '期盼实现 -> 卧室', disappoint: '期盼落空 -> 阁楼',
        internalize: '内化 -> 自我房间', synthesize_user: '整合用户认知',
        self_insight: '自我领悟', self_confuse: '新困惑 -> 阁楼'
    };
    var html = '<div style="padding:16px;"><h3 style="margin:0 0 12px;">认知消化预览</h3>';
    html += '<p style="color:var(--text-muted);font-size:13px;margin-bottom:16px;">以下是角色审视后的判断，勾选要执行的动作：</p>';
    for (var i = 0; i < actions.length; i++) {
        var a = actions[i];
        var label = ACTION_LABELS[a.action] || a.action;
        var room = a.source_room || '';
        html += '<label style="display:block;margin:8px 0;padding:12px;border:1px solid var(--border);border-radius:8px;cursor:pointer;">';
        html += '<input type="checkbox" class="digest-preview-check" value="' + i + '" checked> ';
        html += '<b>' + label + '</b>';
        if (room) html += ' <span style="color:var(--text-muted);font-size:12px;">[' + room + ']</span>';
        html += '<div style="margin-top:6px;font-size:13px;color:var(--text-muted);">原文：' + ((a.source_content || '').substring(0, 100) || '?') + '</div>';
        if (a.reflection) html += '<div style="margin-top:4px;font-size:13px;">→ ' + a.reflection + '</div>';
        if (a.insight) html += '<div style="margin-top:4px;font-size:13px;color:var(--primary);">💡 ' + a.insight.substring(0, 150) + '</div>';
        if (a.category) html += '<div style="margin-top:4px;font-size:12px;color:var(--text-muted);">分类：' + a.category + '</div>';
        html += '</label>';
    }
    html += '<div style="display:flex;gap:8px;justify-content:flex-end;margin-top:16px;">';
    html += '<button class="btn btn-sm" onclick="toggleDigestChecks(false)">全不选</button>';
    html += '<button class="btn btn-sm" onclick="toggleDigestChecks(true)">全选</button>';
    html += '<button class="btn btn-sm" onclick="cancelDigestPreview()">取消</button>';
    html += '<button class="btn btn-primary btn-sm" onclick="confirmDigest()">确认执行</button>';
    html += '</div></div>';
    el.innerHTML = html;
}

function toggleDigestChecks(checked) {
    document.querySelectorAll('.digest-preview-check').forEach(function(cb) { cb.checked = checked; });
}

function cancelDigestPreview() {
    _digestPreviewActions = [];
    clearDigestPreviewArea();
}

let _digestConfirmRunning = false;
async function confirmDigest() {
    if (_digestConfirmRunning) return;
    _digestConfirmRunning = true;
    var checks = document.querySelectorAll('.digest-preview-check:checked');
    var selected = [];
    checks.forEach(function(cb) {
        var idx = parseInt(cb.value);
        if (_digestPreviewActions[idx]) selected.push(_digestPreviewActions[idx]);
    });
    if (!selected.length) { _digestConfirmRunning = false; mpMsg('没有选中任何动作'); return; }
    try {
        mpMsg('正在执行 ' + selected.length + ' 个消化动作...');
        var resp = await fetch('/api/memory-palace/digest/confirm', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({actions: selected})});
        var data = await resp.json();
        if (data.status === 'error') throw new Error(data.error || '执行失败');
        var parts = [];
        if (data.resolved && data.resolved.length) parts.push('化解' + data.resolved.length + '条');
        if (data.deepened && data.deepened.length) parts.push('加深' + data.deepened.length + '条');
        if (data.faded && data.faded.length) parts.push('淡忘' + data.faded.length + '条');
        if (data.fulfilled && data.fulfilled.length) parts.push('期盼实现' + data.fulfilled.length + '条');
        if (data.disappointed && data.disappointed.length) parts.push('期盼落空' + data.disappointed.length + '条');
        if (data.internalized && data.internalized.length) parts.push('内化' + data.internalized.length + '条');
        if (data.synthesized_user && data.synthesized_user.length) parts.push('整合用户认知' + data.synthesized_user.length + '条');
        if (data.self_insights && data.self_insights.length) parts.push('自我领悟' + data.self_insights.length + '条');
        if (data.self_confused && data.self_confused.length) parts.push('新困惑' + data.self_confused.length + '条');
        mpMsg('认知消化完成：' + (parts.length ? parts.join('，') : '无变化'));
        _digestPreviewActions = [];
        await loadMemoryPalace();
    } catch (e) { mpMsg('认知消化执行失败：' + e.message, 'error'); } finally { _digestConfirmRunning = false; }
}

async function runMemoryPalaceConsolidation() {
    if (!confirm('将执行记忆巩固：客厅高重要性记忆晋升卧室，客厅超容量记忆移入阁楼。继续吗？')) return;
    try {
        mpMsg('巩固中...');
        const resp = await fetch('/api/memory-palace/consolidate', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({})});
        const data = await resp.json();
        if (data.error || data.status === 'error') throw new Error(data.error || '巩固失败');
        mpMsg('巩固完成：晋升 ' + Number(data.promoted || 0) + ' 条，淘汰 ' + Number(data.evicted || 0) + ' 条');
        await loadMemoryPalace();
    } catch (e) { mpMsg('巩固失败：' + e.message, 'error'); }
}

async function clearMemoryPalacePins() {
    if (!confirm('将清除所有当前便利贴，只取消置顶，不删除任何记忆。继续吗？')) return;
    const btn = document.getElementById('mpClearPinsBtn');
    const oldText = btn ? btn.textContent : '';
    if (btn) {
        btn.disabled = true;
        btn.textContent = '清除中...';
    }
    try {
        const resp = await fetch('/api/memory-palace/pins/clear', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({})
        });
        const data = await resp.json();
        if (data.error || data.status === 'error') throw new Error(data.error || '清除失败');
        mpMsg('已清除便利贴：' + Number(data.cleared || 0) + ' 条');
        await loadMemoryPalace();
    } catch (e) {
        mpMsg('清除便利贴失败：' + e.message, 'error');
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.textContent = oldText || '清除便利贴';
        }
    }
}

function findMemoryPalaceInlineEditor(id) {
    const editors = document.querySelectorAll('.mp-inline-editor');
    for (let i = 0; i < editors.length; i++) {
        if ((editors[i].dataset.id || '') === id) return editors[i];
    }
    return null;
}

function hideMemoryPalaceInlineEditors() {
    document.querySelectorAll('.mp-inline-editor').forEach(function(el) { el.style.display = 'none'; el.innerHTML = ''; });
}

function openMemoryPalaceCreate() {
    hideMemoryPalaceInlineEditors();
    _mpEditingId = null;
    renderMemoryPalaceEditor(null);
}

function editMemoryPalaceNode(id) {
    const node = _mpNodes.find(n => n.id === id);
    if (!node) return;
    _mpEditingId = id;
    renderMemoryPalaceEditor(node);
}

function renderMemoryPalaceEditor(node) {
    const isEdit = !!node;
    hideMemoryPalaceInlineEditors();
    const globalEditor = document.getElementById('mpEditor');
    if (globalEditor) { globalEditor.style.display = 'none'; globalEditor.innerHTML = ''; }
    const el = isEdit ? findMemoryPalaceInlineEditor(node.id) : globalEditor;
    if (!el) return;
    const roomValue = node ? node.room : (_mpCurrentRoom || 'living_room');
    const roomOptions = _mpRooms.map(r => '<option value=\"' + mpEsc(r.room) + '\" ' + (r.room === roomValue ? 'selected' : '') + '>' + mpEsc(r.label) + '</option>').join('');
    el.style.display = 'block';
    el.innerHTML =
        '<h3 style=\"margin-bottom:14px;\">' + (isEdit ? '编辑记忆节点' : '新增记忆节点') + '</h3>' +
        '<div style=\"display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px;margin-bottom:14px;align-items:end;\">' +
            '<div><label class=\"form-label\">房间</label><select id=\"mpEditRoom\" class=\"select-input\" style=\"width:100%;box-sizing:border-box;\">' + roomOptions + '</select></div>' +
            '<div><label class=\"form-label\">记忆日期</label><input id=\"mpEditDate\" class=\"search-input\" style=\"width:100%;box-sizing:border-box;\" type=\"date\" value=\"' + mpEsc(node ? (node.date || '') : new Date().toISOString().slice(0, 10)) + '\"></div>' +
            '<div><label class=\"form-label\">重要性 1-10</label><input id=\"mpEditImportance\" class=\"search-input\" style=\"width:100%;box-sizing:border-box;\" type=\"number\" min=\"1\" max=\"10\" value=\"' + mpEsc(node ? node.importance : 5) + '\"></div>' +
            '<div><label class=\"form-label\">情绪 mood</label><input id=\"mpEditMood\" class=\"search-input\" style=\"width:100%;box-sizing:border-box;\" value=\"' + mpEsc(node ? (node.mood || 'neutral') : 'neutral') + '\"></div>' +
            '<div><label class=\"form-label\">标签</label><input id=\"mpEditTags\" class=\"search-input\" style=\"width:100%;box-sizing:border-box;\" placeholder=\"用顿号/逗号分隔\" value=\"' + mpEsc(node ? (node.tags || '') : '') + '\"></div>' +
            '<div><label class=\"form-label\">valence</label><input id=\"mpEditValence\" class=\"search-input\" style=\"width:100%;box-sizing:border-box;\" type=\"number\" step=\"0.1\" min=\"-1\" max=\"1\" value=\"' + mpEsc(node && node.valence != null ? node.valence : '') + '\"></div>' +
            '<div><label class=\"form-label\">arousal</label><input id=\"mpEditArousal\" class=\"search-input\" style=\"width:100%;box-sizing:border-box;\" type=\"number\" step=\"0.1\" min=\"-1\" max=\"1\" value=\"' + mpEsc(node && node.arousal != null ? node.arousal : '') + '\"></div>' +
        '</div>' +
        '<div style=\"margin-bottom:12px;\"><label class=\"form-label\">内容</label><textarea id=\"mpEditContent\" class=\"textarea\" style=\"width:100%;box-sizing:border-box;\" rows=\"8\" placeholder=\"用澈的第一人称写下这条记忆...\">' + mpEsc(node ? node.content : '') + '</textarea></div>' +
        '<div style=\"display:flex;gap:8px;flex-wrap:wrap;\">' +
            '<button class=\"btn btn-primary\" onclick=\"saveMemoryPalaceNode()\">保存</button>' +
            '<button class=\"btn btn-secondary\" onclick=\"closeMemoryPalaceEditor()\">取消</button>' +
        '</div>';
    el.scrollIntoView({behavior:'smooth', block:'nearest'});
}

function closeMemoryPalaceEditor() {
    const el = document.getElementById('mpEditor');
    if (el) { el.style.display = 'none'; el.innerHTML = ''; }
    hideMemoryPalaceInlineEditors();
    _mpEditingId = null;
}

async function saveMemoryPalaceNode() {
    const content = document.getElementById('mpEditContent')?.value?.trim() || '';
    if (!content) {
        alert('内容不能为空');
        return;
    }
    const valenceText = document.getElementById('mpEditValence')?.value;
    const arousalText = document.getElementById('mpEditArousal')?.value;
    const payload = {
        content,
        room: document.getElementById('mpEditRoom')?.value || 'living_room',
        date: document.getElementById('mpEditDate')?.value || null,
        tags: document.getElementById('mpEditTags')?.value?.trim() || '',
        importance: Number(document.getElementById('mpEditImportance')?.value || 5),
        mood: document.getElementById('mpEditMood')?.value?.trim() || 'neutral',
        valence: valenceText === '' ? null : Number(valenceText),
        arousal: arousalText === '' ? null : Number(arousalText),
        pinned_until: document.getElementById('mpEditPinnedUntil')?.value || null
    };

    try {
        const url = _mpEditingId ? '/api/memory-palace/nodes/' + encodeURIComponent(_mpEditingId) : '/api/memory-palace/nodes';
        const method = _mpEditingId ? 'PUT' : 'POST';
        const resp = await fetch(url, {
            method,
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify(payload)
        });
        const data = await resp.json();
        if (data.error) throw new Error(data.error);
        mpMsg(_mpEditingId ? '记忆已更新' : '记忆已新增');
        closeMemoryPalaceEditor();
        _mpCurrentRoom = payload.room || _mpCurrentRoom;
        await loadMemoryPalace();
    } catch (e) {
        mpMsg('保存失败：' + e.message, 'error');
    }
}

async function deleteMemoryPalaceNode(id) {
    const node = _mpNodes.find(n => n.id === id);
    if (!confirm('确定删除这条记忆？\\n' + (node ? (node.content || '').slice(0, 60) : id))) return;
    try {
        const resp = await fetch('/api/memory-palace/nodes/' + encodeURIComponent(id), {method: 'DELETE'});
        const data = await resp.json();
        if (data.error) throw new Error(data.error);
        mpMsg('记忆已删除');
        await loadMemoryPalace();
    } catch (e) {
        mpMsg('删除失败：' + e.message, 'error');
    }
}


function mpTagsHtml(tags, color) {
    const parts = String(tags || '').split(/[、,，\n]/).map(t => t.trim()).filter(Boolean);
    if (!parts.length) return '';
    return '<div style="margin-top:8px;display:flex;gap:6px;flex-wrap:wrap;">' +
        parts.slice(0, 8).map(t => '<span style="font-size:12px;padding:3px 8px;border-radius:999px;background:' + color + '18;color:' + color + ';">#' + mpEsc(t) + '</span>').join('') +
        '</div>';
}

async function loadMemoryPalaceEventBoxes() {
    const listEl = document.getElementById('mpEventBoxList');
    if (!listEl) return;
    listEl.innerHTML = '<div style="color:var(--text-muted);padding:12px;">加载中...</div>';
    try {
        const resp = await fetch('/api/memory-palace/event-boxes?limit=100');
        const data = await resp.json();
        if (data.error || data.status === 'error') throw new Error(data.error || '加载失败');
        _mpEventBoxes = data.boxes || [];
        if (_mpCurrentEventBoxId && !_mpEventBoxes.some(b => b.id === _mpCurrentEventBoxId)) {
            _mpCurrentEventBoxId = null;
        }
        renderMemoryPalaceEventBoxes();
        if (_mpCurrentEventBoxId) {
            await loadMemoryPalaceEventBoxDetail(_mpCurrentEventBoxId);
        } else {
            const detailEl = document.getElementById('mpEventBoxDetail');
            if (detailEl) detailEl.innerHTML = '<div style="color:var(--text-muted);padding:16px;border:1px dashed var(--border-color);border-radius:10px;">选择一个事件盒查看详情。</div>';
        }
    } catch (e) {
        listEl.innerHTML = '<div style="color:#dc2626;padding:12px;">加载事件盒失败：' + mpEsc(e.message) + '</div>';
    }
}

function renderMemoryPalaceEventBoxes() {
    const el = document.getElementById('mpEventBoxList');
    if (!el) return;
    if (!_mpEventBoxes.length) {
        el.innerHTML = '<div style="color:var(--text-muted);padding:12px;">还没有事件盒。新的 relatedTo / sameAs 关联入库后会出现在这里。</div>';
        return;
    }
    el.innerHTML = _mpEventBoxes.map(box => {
        const active = box.id === _mpCurrentEventBoxId;
        const tags = mpTagsHtml(box.tags || '', active ? '#2563eb' : '#64748b');
        const updated = String(box.updated_at || '').slice(0, 19).replace('T', ' ');
        return '<button type="button" class="mp-event-box-card" data-id="' + mpEsc(box.id) + '" ' +
            'style="text-align:left;width:100%;border:1px solid ' + (active ? '#2563eb' : 'var(--border-color)') + ';background:' + (active ? '#eff6ff' : '#fff') + ';border-radius:10px;padding:12px;cursor:pointer;">' +
            '<div style="font-weight:800;color:' + (active ? '#1d4ed8' : 'var(--text-primary)') + ';">' + mpEsc(box.name || '未命名事件') + '</div>' +
            '<div style="font-size:12px;color:var(--text-muted);margin-top:4px;">live ' + Number(box.live_count || 0) + ' · archived ' + Number(box.archived_count || 0) + ' · 压缩 ' + Number(box.compression_count || 0) + '</div>' +
            tags +
            '<div style="font-size:11px;color:var(--text-muted);margin-top:8px;">' + mpEsc(updated || box.id) + '</div>' +
        '</button>';
    }).join('');
}

async function compressMemoryPalaceEventBoxes() {
    if (!confirm('将调用记忆模型压缩达到阈值的事件盒，压缩后的 live 节点会归档并生成 summary。继续吗？')) return;
    try {
        mpMsg('正在压缩可压缩事件盒...');
        const resp = await fetch('/api/memory-palace/event-boxes/compress', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({})
        });
        const data = await resp.json();
        if (data.error || data.status === 'error') throw new Error(data.error || '压缩失败');
        mpMsg('事件盒压缩完成：压缩 ' + Number(data.compressed || 0) + ' 个');
        await loadMemoryPalace();
    } catch (e) {
        mpMsg('事件盒压缩失败：' + e.message, 'error');
    }
}

async function selectMemoryPalaceEventBox(id) {
    _mpCurrentEventBoxId = id || null;
    renderMemoryPalaceEventBoxes();
    if (id) await loadMemoryPalaceEventBoxDetail(id);
}


function mpEventBoxTimeText(value) {
    if (!value) return '';
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return String(value).slice(0, 19).replace('T', ' ');
    const pad = n => String(n).padStart(2, '0');
    return d.getFullYear() + '-' + pad(d.getMonth() + 1) + '-' + pad(d.getDate()) + ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes());
}

function mpEventBoxSectionHtml(title, nodes, emptyText, collapsed) {
    const body = (nodes || []).map(n => { const acts = []; if (!n.is_box_summary) acts.push('<button class=\"btn btn-secondary btn-sm mp-remove-node-from-box\" data-id=\"' + mpEsc(n.id || '') + '\">移出盒</button>'); if (collapsed && n.archived) acts.push('<button class=\"btn btn-secondary btn-sm mp-revive-archived-node\" data-id=\"' + mpEsc(n.id || '') + '\">复活</button>'); return mpEventBoxNodeHtml(n, {actions: acts.length ? '<div style=\"margin-top:8px;display:flex;justify-content:flex-end;gap:8px;flex-wrap:wrap;\">' + acts.join('') + '</div>' : ''}); }).join('') || '<div style="color:var(--text-muted);padding:12px;border:1px dashed var(--border-color);border-radius:10px;">' + mpEsc(emptyText || '暂无节点') + '</div>';
    if (collapsed) {
        return '<details style="margin-top:12px;border:1px solid var(--border-color);border-radius:12px;background:#fff;">' +
            '<summary style="cursor:pointer;padding:12px 14px;font-weight:800;">' + mpEsc(title) + ' <span style="color:var(--text-muted);font-weight:500;">(' + Number((nodes || []).length) + ')</span></summary>' +
            '<div style="display:flex;flex-direction:column;gap:10px;padding:0 14px 14px;">' + body + '</div>' +
        '</details>';
    }
    return '<div style="margin-top:12px;">' +
        '<div style="font-weight:800;margin-bottom:8px;">' + mpEsc(title) + ' <span style="color:var(--text-muted);font-weight:500;">(' + Number((nodes || []).length) + ')</span></div>' +
        '<div style="display:flex;flex-direction:column;gap:10px;">' + body + '</div>' +
    '</div>';
}

function mpEventBoxStatHtml(label, value, color) {
    return '<div style="padding:8px 10px;border:1px solid var(--border-color);border-radius:10px;background:#f8fafc;min-width:82px;">' +
        '<div style="font-size:11px;color:var(--text-muted);">' + mpEsc(label) + '</div>' +
        '<div style="font-weight:800;color:' + (color || 'var(--text-color)') + ';margin-top:2px;">' + mpEsc(value) + '</div>' +
    '</div>';
}

function mpEventBoxNodeHtml(node, opts) {
    opts = opts || {};
    const room = mpRoomMeta(node.room);
    const color = node.is_box_summary ? '#7c3aed' : (node.archived ? '#64748b' : (room.color || '#64748b'));
    const label = node.is_box_summary ? 'summary' : (node.archived ? 'archived' : 'live');
    const date = node.date || String(node.created_at || '').slice(0, 10);
    return '<div style="border:1px solid var(--border-color);border-left:4px solid ' + color + ';border-radius:10px;padding:12px;background:#fff;">' +
        '<div style="display:flex;justify-content:space-between;gap:8px;align-items:flex-start;">' +
            '<div style="font-size:12px;color:var(--text-muted);">' + mpEsc(label) + ' · ' + mpEsc(room.label || node.room || '') + ' · ' + mpEsc(date || '') + '</div>' +
            '<div style="font-size:12px;color:var(--text-muted);white-space:nowrap;">importance ' + mpEsc(node.importance || 5) + ' · ' + mpEsc(node.mood || 'neutral') + '</div>' +
        '</div>' +
        '<div style="white-space:pre-wrap;line-height:1.6;margin-top:8px;font-size:14px;">' + mpEsc(node.content || '') + '</div>' +
        mpTagsHtml(node.tags || '', color) +
        (opts.actions || '') +
    '</div>';
}

async function loadMemoryPalaceEventBoxDetail(id) {
    const el = document.getElementById('mpEventBoxDetail');
    if (!el) return;
    el.innerHTML = '<div style="color:var(--text-muted);padding:16px;">加载中...</div>';
    try {
        const resp = await fetch('/api/memory-palace/event-boxes/' + encodeURIComponent(id));
        const data = await resp.json();
        if (data.error || data.status === 'error') throw new Error(data.error || '加载失败');
        const box = data.box || {};
        const nodes = data.nodes || [];
        const summaryNodes = nodes.filter(n => n.is_box_summary);
        const liveNodes = nodes.filter(n => !n.is_box_summary && !n.archived);
        const archivedNodes = nodes.filter(n => n.archived && !n.is_box_summary);
        const tags = mpTagsHtml(box.tags || '', '#2563eb');
        const statusBadge = box.sealed
            ? '<span style="display:inline-flex;align-items:center;padding:3px 8px;border-radius:999px;background:#fee2e2;color:#991b1b;font-size:12px;font-weight:700;">sealed</span>'
            : '<span style="display:inline-flex;align-items:center;padding:3px 8px;border-radius:999px;background:#dcfce7;color:#166534;font-size:12px;font-weight:700;">open</span>';
        const metaLines = [];
        metaLines.push('ID: ' + (box.id || ''));
        if (box.predecessor_box_id) metaLines.push('前任盒: ' + box.predecessor_box_id);
        if (box.summary_node_id) metaLines.push('summary: ' + box.summary_node_id);
        if (box.last_compressed_at) metaLines.push('最后压缩: ' + mpEventBoxTimeText(box.last_compressed_at));
        const stats = [
            mpEventBoxStatHtml('live', Number(box.live_count || liveNodes.length), '#059669'),
            mpEventBoxStatHtml('archived', Number(box.archived_count || archivedNodes.length), '#64748b'),
            mpEventBoxStatHtml('压缩次数', Number(box.compression_count || 0), '#7c3aed')
        ].join('');
        el.innerHTML = '<div style="border:1px solid var(--border-color);border-radius:12px;padding:14px;background:#fff;">' +
            '<div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;flex-wrap:wrap;">' +
                '<div style="min-width:220px;">' +
                    '<div style="display:flex;gap:8px;align-items:center;flex-wrap:wrap;">' +
                        '<div style="font-weight:900;font-size:18px;">' + mpEsc(box.name || '未命名事件') + '</div>' + statusBadge +
                    '</div>' +
                    '<div style="font-size:12px;color:var(--text-muted);margin-top:6px;line-height:1.6;white-space:pre-wrap;">' + mpEsc(metaLines.join('\n')) + '</div>' +
                    '<div style="display:flex;gap:8px;flex-wrap:wrap;margin-top:10px;">' +
                        '<button class="btn btn-secondary btn-sm mp-compress-current-box" data-id="' + mpEsc(box.id || '') + '">压缩此盒</button>' +
                        '<button class="btn btn-secondary btn-sm mp-toggle-sealed-box" data-id="' + mpEsc(box.id || '') + '" data-sealed="' + (box.sealed ? 'false' : 'true') + '">' + (box.sealed ? '解除封盒' : '封盒') + '</button>' +
                        '<button class="btn btn-secondary btn-sm mp-unbind-live-box" data-id="' + mpEsc(box.id || '') + '">清空 live</button>' +
                    '</div>' +
                '</div>' +
                '<div style="display:flex;gap:8px;flex-wrap:wrap;justify-content:flex-end;">' + stats + '</div>' +
            '</div>' +
            tags +
            mpEventBoxSectionHtml('Summary', summaryNodes, '尚未生成 summary。达到压缩阈值后会在这里置顶显示。') +
            mpEventBoxSectionHtml('Live 节点', liveNodes, '暂无 live 节点。') +
            mpEventBoxSectionHtml('Archived 节点', archivedNodes, '暂无 archived 节点。', true) +
        '</div>';
    } catch (e) {
        el.innerHTML = '<div style="color:#dc2626;padding:16px;border:1px solid #fecaca;border-radius:10px;background:#fff;">加载事件盒详情失败：' + mpEsc(e.message) + '</div>';
    }
}


async function compressCurrentMemoryPalaceEventBox(id) {
    id = id || _mpCurrentEventBoxId;
    if (!id) return;
    if (!confirm('将立即压缩这个事件盒的 live 节点。继续吗？')) return;
    try {
        const resp = await fetch('/api/memory-palace/event-boxes/compress', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({box_ids:[id], threshold:2})});
        const data = await resp.json();
        if (data.error || data.status === 'error') throw new Error(data.error || '压缩失败');
        mpMsg('事件盒压缩完成：压缩 ' + Number(data.compressed || 0) + ' 个');
        await loadMemoryPalaceEventBoxes();
        await loadMemoryPalaceEventBoxDetail(id);
    } catch (e) { mpMsg('压缩此盒失败：' + e.message, 'error'); }
}

async function setMemoryPalaceEventBoxSealed(id, sealed) {
    id = id || _mpCurrentEventBoxId;
    if (!id) return;
    if (!confirm((sealed ? '封盒后后续相关记忆会开延续盒。' : '解除封盒后后续相关记忆可能继续写入此盒。') + '继续吗？')) return;
    try {
        const resp = await fetch('/api/memory-palace/event-boxes/' + encodeURIComponent(id), {method:'PATCH', headers:{'Content-Type':'application/json'}, body:JSON.stringify({sealed:!!sealed})});
        const data = await resp.json();
        if (data.error || data.status === 'error') throw new Error(data.error || '更新失败');
        mpMsg(sealed ? '已封盒' : '已解除封盒');
        await loadMemoryPalaceEventBoxes();
        await loadMemoryPalaceEventBoxDetail(id);
    } catch (e) { mpMsg('更新事件盒失败：' + e.message, 'error'); }
}

async function unbindMemoryPalaceEventBoxLive(id) {
    id = id || _mpCurrentEventBoxId;
    if (!id) return;
    if (!confirm('将把此盒所有 live 节点移出事件盒，summary 和 archived 保持不动。继续吗？')) return;
    try {
        const resp = await fetch('/api/memory-palace/event-boxes/' + encodeURIComponent(id) + '/unbind-live', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({})});
        const data = await resp.json();
        if (data.error || data.status === 'error') throw new Error(data.error || '清空失败');
        mpMsg('已移出 live 节点：' + Number(data.moved || 0) + ' 条');
        if (data.deleted) _mpCurrentEventBoxId = null;
        await loadMemoryPalace();
        if (_mpCurrentEventBoxId) await loadMemoryPalaceEventBoxDetail(_mpCurrentEventBoxId);
    } catch (e) { mpMsg('清空 live 池失败：' + e.message, 'error'); }
}

async function reviveMemoryPalaceArchivedNode(id) {
    if (!id) return;
    if (!confirm('将这条 archived 记忆复活为 live 节点。继续吗？')) return;
    try {
        const resp = await fetch('/api/memory-palace/nodes/' + encodeURIComponent(id) + '/revive', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({})});
        const data = await resp.json();
        if (data.error || data.status === 'error') throw new Error(data.error || '复活失败');
        mpMsg('已复活 archived 节点');
        await loadMemoryPalaceEventBoxes();
        if (_mpCurrentEventBoxId) await loadMemoryPalaceEventBoxDetail(_mpCurrentEventBoxId);
        await loadMemoryPalaceNodes(_mpCurrentRoom);
    } catch (e) { mpMsg('复活 archived 节点失败：' + e.message, 'error'); }
}


async function addMemoryPalaceNodeToCurrentBox(nodeId) {
    if (!_mpCurrentEventBoxId) { mpMsg('请先选择一个事件盒', 'error'); return; }
    if (!nodeId) return;
    if (!confirm('将这条记忆加入当前事件盒。继续吗？')) return;
    try {
        const resp = await fetch('/api/memory-palace/event-boxes/' + encodeURIComponent(_mpCurrentEventBoxId) + '/add-node', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({node_id:nodeId})});
        const data = await resp.json();
        if (data.error || data.status === 'error') throw new Error(data.error || '加入失败');
        mpMsg('已加入当前事件盒');
        await loadMemoryPalaceEventBoxes();
        await loadMemoryPalaceEventBoxDetail(_mpCurrentEventBoxId);
        await loadMemoryPalaceNodes(_mpCurrentRoom);
    } catch (e) { mpMsg('加入事件盒失败：' + e.message, 'error'); }
}

async function removeMemoryPalaceNodeFromBox(nodeId) {
    if (!nodeId) return;
    if (!confirm('将这条记忆移出事件盒，恢复为独立记忆。继续吗？')) return;
    try {
        const resp = await fetch('/api/memory-palace/nodes/' + encodeURIComponent(nodeId) + '/remove-from-box', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({})});
        const data = await resp.json();
        if (data.error || data.status === 'error') throw new Error(data.error || '移出失败');
        mpMsg('已移出事件盒');
        if (data.deleted) _mpCurrentEventBoxId = null;
        await loadMemoryPalaceEventBoxes();
        if (_mpCurrentEventBoxId) await loadMemoryPalaceEventBoxDetail(_mpCurrentEventBoxId);
        await loadMemoryPalaceNodes(_mpCurrentRoom);
    } catch (e) { mpMsg('移出事件盒失败：' + e.message, 'error'); }
}

async function manualBindMemoryPalaceNode(nodeId) {
    if (!nodeId) return;
    const otherId = prompt('输入另一条记忆节点 ID，用来与当前节点绑定成事件盒');
    if (!otherId) return;
    const name = prompt('事件盒名称（可留空）') || '';
    const tags = prompt('事件标签（可留空，用逗号/顿号分隔）') || '';
    try {
        const resp = await fetch('/api/memory-palace/event-boxes/manual-bind', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({node_id:nodeId, existing_node_id:otherId.trim(), eventName:name.trim(), eventTags:tags.trim()})});
        const data = await resp.json();
        if (data.error || data.status === 'error') throw new Error(data.error || '绑定失败');
        mpMsg('手动绑定完成：触达事件盒 ' + Number(data.event_boxes || 0) + ' 个');
        await loadMemoryPalace();
    } catch (e) { mpMsg('手动绑定失败：' + e.message, 'error'); }
}

function initMemoryPalaceInteractions() {
    const section = document.getElementById('section-memory-palace');
    if (!section || section.dataset.mpBound === '1') return;
    section.dataset.mpBound = '1';
    section.addEventListener('click', (event) => {
        const backfillBtn = event.target.closest('#mpBackfillBtn');
        if (backfillBtn) {
            event.preventDefault();
            backfillMemoryPalaceEmbeddings();
            return;
        }
        const roomCard = event.target.closest('.mp-room-card');
        if (roomCard) {
            selectMemoryPalaceRoom(roomCard.dataset.room || '');
            return;
        }
        const addCurrentBoxBtn = event.target.closest('.mp-add-node-current-box');
        if (addCurrentBoxBtn) { addMemoryPalaceNodeToCurrentBox(addCurrentBoxBtn.dataset.id || ''); return; }
        const manualBindBtn = event.target.closest('.mp-manual-bind-node');
        if (manualBindBtn) { manualBindMemoryPalaceNode(manualBindBtn.dataset.id || ''); return; }
        const removeFromBoxBtn = event.target.closest('.mp-remove-node-from-box');
        if (removeFromBoxBtn) { removeMemoryPalaceNodeFromBox(removeFromBoxBtn.dataset.id || ''); return; }
        const editBtn = event.target.closest('.mp-edit-node');
        if (editBtn) {
            editMemoryPalaceNode(editBtn.dataset.id || '');
            return;
        }
        const deleteBtn = event.target.closest('.mp-delete-node');
        if (deleteBtn) {
            deleteMemoryPalaceNode(deleteBtn.dataset.id || '');
            return;
        }
        const compressBoxBtn = event.target.closest('.mp-compress-current-box');
        if (compressBoxBtn) { compressCurrentMemoryPalaceEventBox(compressBoxBtn.dataset.id || ''); return; }
        const toggleSealedBtn = event.target.closest('.mp-toggle-sealed-box');
        if (toggleSealedBtn) { setMemoryPalaceEventBoxSealed(toggleSealedBtn.dataset.id || '', toggleSealedBtn.dataset.sealed === 'true'); return; }
        const unbindLiveBtn = event.target.closest('.mp-unbind-live-box');
        if (unbindLiveBtn) { unbindMemoryPalaceEventBoxLive(unbindLiveBtn.dataset.id || ''); return; }
        const reviveBtn = event.target.closest('.mp-revive-archived-node');
        if (reviveBtn) { reviveMemoryPalaceArchivedNode(reviveBtn.dataset.id || ''); return; }
        const eventBoxBtn = event.target.closest('.mp-event-box-card');
        if (eventBoxBtn) {
            selectMemoryPalaceEventBox(eventBoxBtn.dataset.id || '');
        }
    });

    section.addEventListener('change', (event) => {
        const showIds = event.target.closest('#mpShowNodeIds');
        if (showIds) {
            _mpShowNodeIds = !!showIds.checked;
            renderMemoryPalaceNodes();
        }
    });
}

async function debugRetrieveMemoryPalace() {
    const queryEl = document.getElementById('mpDebugQuery');
    const limitEl = document.getElementById('mpDebugLimit');
    const roomEl = document.getElementById('mpDebugRoom');
    const resultEl = document.getElementById('mpDebugResult');
    const query = (queryEl?.value || '').trim();
    if (!query) {
        mpMsg('请输入调试 query', 'error');
        return;
    }
    if (resultEl) {
        resultEl.style.display = 'block';
        resultEl.innerHTML = '<div style="color:var(--text-muted);">召回中...</div>';
    }
    try {
        const resp = await fetch('/api/memory-palace/debug-retrieve', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({
                query,
                limit: Number(limitEl?.value || 5),
                room: roomEl?.value || '',
                messages: [{role: 'user', content: query}]
            })
        });
        const data = await resp.json();
        if (data.error) throw new Error(data.error);
        const nodes = data.nodes || [];
        const cards = nodes.map((node, idx) => {
            const room = mpRoomMeta(node.room);
            const score = node.score == null ? '' : Number(node.score).toFixed(3);
            const flags = [];
            if (idx < Number(data.pinned_count || 0)) flags.push('📌便利贴');
            if (node.activation) flags.push('联想扩展');
            return '<div style="border:1px solid var(--border-color);border-radius:10px;padding:10px;margin-top:8px;background:#fff;">' +
                '<div style="display:flex;justify-content:space-between;gap:8px;font-size:12px;color:var(--text-muted);">' +
                    '<span>#' + (idx + 1) + ' · ' + mpEsc(room.label || node.room) + ' · score ' + mpEsc(score) + (flags.length ? ' · ' + mpEsc(flags.join(' · ')) : '') + '</span>' +
                    '<span>' + mpEsc(node.date || '') + '</span>' +
                '</div>' +
                '<div style="margin-top:6px;white-space:pre-wrap;line-height:1.55;">' + mpEsc(node.content || '') + '</div>' +
                '<div style="margin-top:6px;font-size:12px;color:var(--text-muted);">importance ' + mpEsc(node.importance || 5) + ' · ' + mpEsc(node.mood || 'neutral') + '</div>' +
            '</div>';
        }).join('');
        if (resultEl) {
            resultEl.innerHTML = '<div style="font-weight:700;margin-bottom:8px;">命中 ' + Number(data.count || 0) + ' 条（便利贴 ' + Number(data.pinned_count || 0) + ' 条）</div>' +
                '<details style="margin-bottom:10px;"><summary style="cursor:pointer;color:var(--primary-color);">查看注入 Markdown</summary>' +
                '<pre style="white-space:pre-wrap;background:#f8fafc;border:1px solid var(--border-color);border-radius:10px;padding:10px;margin-top:8px;max-height:260px;overflow:auto;">' + mpEsc(data.markdown || '') + '</pre></details>' +
                (cards || '<div style="color:var(--text-muted);">没有命中记忆。</div>');
        }
        mpMsg('✅ 调试召回完成：命中 ' + Number(data.count || 0) + ' 条');
    } catch (e) {
        mpMsg('调试召回失败：' + e.message, 'error');
        if (resultEl) resultEl.innerHTML = '<div style="color:#dc2626;">' + mpEsc(e.message) + '</div>';
    }
}

function initMemoryPalacePage() {
    initMemoryPalaceInteractions();
    document.querySelectorAll('.nav-item[data-section=\"memory-palace\"]').forEach(item => {
        item.addEventListener('click', () => setTimeout(loadMemoryPalace, 0));
    });
    if (document.getElementById('section-memory-palace')?.classList.contains('active')) {
        loadMemoryPalace();
    }
}

document.addEventListener('DOMContentLoaded', initMemoryPalacePage);

let _mpBackfillRunning = false;
let _mpBackfillPollTimer = null;

function setMemoryPalaceBackfillButton(running) {
    const btn = document.getElementById('mpBackfillBtn');
    if (!btn) return;
    btn.disabled = !!running;
    btn.textContent = running ? '补全中...' : '补全向量';
}

function showMemoryPalaceBackfillStatus(text, type) {
    mpMsg(text || '', type);
}


async function showMemoryPalaceVectorStats() {
    const btn = document.getElementById('mpVectorStatsBtn');
    if (btn) {
        btn.disabled = true;
        btn.textContent = '查询中...';
    }
    showMemoryPalaceBackfillStatus('正在查询当前向量数量...');
    try {
        const resp = await fetch('/api/memory-palace/vector-stats');
        const data = await resp.json();
        if (data.error) throw new Error(data.error);
        showMemoryPalaceBackfillStatus(
            '📊 当前向量：节点 ' + (data.total_nodes || 0) +
            ' 条，有效向量 ' + (data.total_vectors || 0) +
            ' 条，缺失/空向量 ' + (data.missing_vectors || 0) +
            ' 条，空向量行 ' + (data.invalid_vector_rows || 0) + ' 条'
        );
    } catch (e) {
        showMemoryPalaceBackfillStatus('❌ 查询向量数量失败：' + e.message, 'error');
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.textContent = '查看向量数';
        }
    }
}

async function backfillMemoryPalaceEmbeddings() {
    if (_mpBackfillRunning) return;
    if (_mpBackfillPollTimer) {
        clearTimeout(_mpBackfillPollTimer);
        _mpBackfillPollTimer = null;
    }
    _mpBackfillRunning = true;
    setMemoryPalaceBackfillButton(true);
    showMemoryPalaceBackfillStatus('正在检查缺失向量...');
    try {
        const resp = await fetch('/api/memory-palace/backfill-embeddings', {
            method:'POST',
            headers:{'Content-Type':'application/json'},
            body:JSON.stringify({})
        });
        const data = await resp.json();
        if (data.error) throw new Error(data.error);
        if (data.status === 'done') {
            const st = data.stats || {};
            showMemoryPalaceBackfillStatus('✅ ' + (data.message || ('向量索引完整：节点 ' + (st.total_nodes || 0) + ' 条，向量 ' + (st.total_vectors || 0) + ' 条，缺失 ' + (st.missing_vectors || 0) + ' 条')));
            _mpBackfillRunning = false;
            setMemoryPalaceBackfillButton(false);
            return;
        }

        showMemoryPalaceBackfillStatus('⏳ ' + (data.message || ('开始补全向量，共 ' + (data.total || 0) + ' 个节点待处理')));
        const poll = async () => {
            try {
                const r = await fetch('/api/memory-palace/backfill-embeddings/status');
                const s = await r.json();
                if (s.error) {
                    showMemoryPalaceBackfillStatus('❌ 向量补全失败：' + s.error, 'error');
                    _mpBackfillRunning = false;
                    setMemoryPalaceBackfillButton(false);
                    return;
                }
                if (s.running) {
                    showMemoryPalaceBackfillStatus('⏳ ' + (s.message || ('正在补全向量：' + (s.done || 0) + '/' + (s.total || 0))));
                    _mpBackfillPollTimer = setTimeout(poll, 1500);
                    return;
                }
                const doneMsg = s.message || ('向量补全完成：新增 ' + (s.inserted || 0) + ' 条，跳过/已有 ' + (s.skipped || 0) + ' 条，失败 ' + (s.failed || 0) + ' 条');
                showMemoryPalaceBackfillStatus((s.failed || 0) > 0 ? ('⚠️ ' + doneMsg) : ('✅ ' + doneMsg), (s.failed || 0) > 0 ? 'error' : undefined);
                _mpBackfillRunning = false;
                setMemoryPalaceBackfillButton(false);
                await loadMemoryPalace();
            } catch(e) {
                showMemoryPalaceBackfillStatus('❌ 查询补全进度失败：' + e.message, 'error');
                _mpBackfillRunning = false;
                setMemoryPalaceBackfillButton(false);
            }
        };
        _mpBackfillPollTimer = setTimeout(poll, 800);
    } catch(e) {
        showMemoryPalaceBackfillStatus('❌ 向量补全失败：' + e.message, 'error');
        _mpBackfillRunning = false;
        setMemoryPalaceBackfillButton(false);
    }
}
