let _mpRooms = [];
let _mpNodes = [];
let _mpCurrentRoom = '';
let _mpEditingId = null;

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

async function loadMemoryPalace() {
    const root = document.getElementById('section-memory-palace');
    if (!root) return;

    try {
        mpMsg('');
        const resp = await fetch('/api/memory-palace/rooms');
        const data = await resp.json();
        if (data.error) throw new Error(data.error);
        _mpRooms = data.rooms || [];
        renderMemoryPalaceRooms();
        await loadMemoryPalaceNodes(_mpCurrentRoom);
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
        return '<div class=\"card\" style=\"padding:16px;border-top:4px solid ' + color + ';\">' +
            '<div style=\"display:flex;justify-content:space-between;gap:10px;margin-bottom:10px;\">' +
                '<div>' +
                    '<div style=\"font-weight:800;color:' + color + ';\">' + mpEsc(room.label || node.room) + '</div>' +
                    '<div style=\"font-size:12px;color:var(--text-muted);margin-top:2px;\">importance ' + mpEsc(node.importance || 5) + ' · ' + mpEsc(node.mood || 'neutral') + '</div>' +
                '</div>' +
                '<div style=\"display:flex;gap:6px;align-items:flex-start;\">' +
                    '<button class=\"mp-edit-node\" data-id=\"' + mpEsc(node.id) + '\" style=\"padding:4px 8px;border:1px solid var(--border-color);border-radius:6px;background:#fff;cursor:pointer;font-size:12px;\">编辑</button>' +
                    '<button class=\"mp-delete-node\" data-id=\"' + mpEsc(node.id) + '\" style=\"padding:4px 8px;border:1px solid #dc2626;color:#dc2626;border-radius:6px;background:#fff;cursor:pointer;font-size:12px;\">删除</button>' +
                '</div>' +
            '</div>' +
            '<div style=\"white-space:pre-wrap;line-height:1.65;font-size:14px;\">' + mpEsc(node.content || '') + '</div>' +
            (tags.length ? '<div style=\"margin-top:12px;display:flex;gap:6px;flex-wrap:wrap;\">' + tags.map(t => '<span style=\"font-size:12px;padding:3px 8px;border-radius:999px;background:' + color + '18;color:' + color + ';\">#' + mpEsc(t) + '</span>').join('') + '</div>' : '') +
            '<div style=\"margin-top:10px;font-size:12px;color:var(--text-muted);\">' + mpEsc((node.created_at || '').slice(0, 19).replace('T', ' ')) + (node.access_count ? ' · 访问 ' + node.access_count : '') + '</div>' +
        '</div>';
    }).join('');
}


async function extractRecentMemoryPalace(limit) {
    limit = limit || 50;
    if (!confirm('将调用记忆提取模型处理最近 ' + limit + ' 条对话，并写入记忆宫殿。继续吗？')) return;
    const btn = document.getElementById('mpExtractRecentBtn');
    const oldText = btn ? btn.textContent : '';
    if (btn) {
        btn.disabled = true;
        btn.textContent = '处理中...';
    }
    try {
        mpMsg('正在处理最近 ' + limit + ' 条对话，请稍候...');
        const resp = await fetch('/api/memory-palace/extract-recent', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({limit})
        });
        const data = await resp.json();
        if (data.error || data.status === 'error') throw new Error(data.error || '提取失败');
        mpMsg('处理完成：读取 ' + (data.processed_messages || 0) + ' 条，对模型提取 ' + (data.extracted || 0) + ' 条，入库 ' + (data.created || 0) + ' 条，向量化 ' + (data.embedded || 0) + ' 条。');
        await loadMemoryPalace();
    } catch (e) {
        mpMsg('处理失败：' + e.message, 'error');
    } finally {
        if (btn) {
            btn.disabled = false;
            btn.textContent = oldText || '处理最近50条';
        }
    }
}

function openMemoryPalaceCreate() {
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
    const el = document.getElementById('mpEditor');
    if (!el) return;
    const isEdit = !!node;
    const roomValue = node ? node.room : (_mpCurrentRoom || 'living_room');
    const roomOptions = _mpRooms.map(r => '<option value=\"' + mpEsc(r.room) + '\" ' + (r.room === roomValue ? 'selected' : '') + '>' + mpEsc(r.label) + '</option>').join('');
    el.style.display = 'block';
    el.innerHTML =
        '<h3 style=\"margin-bottom:14px;\">' + (isEdit ? '编辑记忆节点' : '新增记忆节点') + '</h3>' +
        '<div style=\"display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:14px;margin-bottom:14px;align-items:end;\">' +
            '<div><label class=\"form-label\">房间</label><select id=\"mpEditRoom\" class=\"select-input\" style=\"width:100%;box-sizing:border-box;\">' + roomOptions + '</select></div>' +
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
    if (el) el.style.display = 'none';
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
        tags: document.getElementById('mpEditTags')?.value?.trim() || '',
        importance: Number(document.getElementById('mpEditImportance')?.value || 5),
        mood: document.getElementById('mpEditMood')?.value?.trim() || 'neutral',
        valence: valenceText === '' ? null : Number(valenceText),
        arousal: arousalText === '' ? null : Number(arousalText)
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


function initMemoryPalaceInteractions() {
    const section = document.getElementById('section-memory-palace');
    if (!section || section.dataset.mpBound === '1') return;
    section.dataset.mpBound = '1';
    section.addEventListener('click', (event) => {
        const roomCard = event.target.closest('.mp-room-card');
        if (roomCard) {
            selectMemoryPalaceRoom(roomCard.dataset.room || '');
            return;
        }
        const editBtn = event.target.closest('.mp-edit-node');
        if (editBtn) {
            editMemoryPalaceNode(editBtn.dataset.id || '');
            return;
        }
        const deleteBtn = event.target.closest('.mp-delete-node');
        if (deleteBtn) {
            deleteMemoryPalaceNode(deleteBtn.dataset.id || '');
        }
    });
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
