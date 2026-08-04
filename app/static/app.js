const state = {
  user: null,
  servers: [],
  primaryId: null,
  streams: [],
  selectedServerId: null,
  currentStream: null,
  currentServer: null,
  currentView: 'streams',
  compareStream: null,
  draggedRow: null,
  monitorPayload: null,
  monitorEventSource: null,
  monitorHistory: [],
  monitorSort: 'server',
  selectedStreams: new Set(),
  syncItems: [],
  sourceItems: [],
  sourceActionItem: null,
  backupItems: [],
  auditItems: [],
  alertItems: [],
  monitorRange: 'live',
  loadPayload: null,
  loadEventSource: null,
  loadHistory: [],
  loadRange: 'live',
  loadMetric: 'network',
  loadServerId: 'all',
  clusterPayload: null,
  clusterSettings: null,
  clusterTimer: null,
  placementEnabled: false,
  placementItems: [],
  placementSelected: new Set(),
  placementServerCounts: [],
  m3uPreview: null,
};

const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];

async function api(path, options = {}) {
  const response = await fetch(path, {
    credentials: 'same-origin',
    headers: { 'Content-Type': 'application/json', ...(options.headers || {}) },
    ...options,
  });
  const contentType = response.headers.get('content-type') || '';
  const body = contentType.includes('application/json') ? await response.json() : await response.text();
  if (response.status === 401) {
    showLogin();
    throw new Error(body?.detail || 'Требуется авторизация');
  }
  if (!response.ok) throw new Error(body?.detail || body?.error || `HTTP ${response.status}`);
  return body;
}

function toast(message, type = 'success', timeout = 3800) {
  const node = document.createElement('div');
  node.className = `toast ${type}`;
  node.textContent = message;
  $('#toast-root').append(node);
  setTimeout(() => node.remove(), timeout);
}

function showLogin() {
  $('#app-view').classList.add('hidden');
  $('#login-view').classList.remove('hidden');
}

function showApp() {
  $('#login-view').classList.add('hidden');
  $('#app-view').classList.remove('hidden');
}

function setBusy(button, busy, label = null) {
  if (!button) return;
  if (busy) {
    button.dataset.original = button.innerHTML;
    button.disabled = true;
    button.textContent = label || 'Сохранение…';
  } else {
    button.disabled = false;
    button.innerHTML = button.dataset.original || button.innerHTML;
  }
}

function escapeHtml(value = '') {
  return String(value).replace(/[&<>'"]/g, ch => ({ '&': '&amp;', '<': '&lt;', '>': '&gt;', "'": '&#39;', '"': '&quot;' }[ch]));
}

function formatStatus(stream) {
  if (stream.disabled) return ['waiting', 'Отключён'];
  if (stream.alive) return ['alive', 'Активен'];
  if (stream.status === 'waiting') return ['waiting', 'Ожидает'];
  return ['', stream.status || 'Неактивен'];
}

function refreshStreamMetrics() {
  $('#metric-total').textContent = state.streams.length;
  $('#metric-alive').textContent = state.streams.filter(stream => stream.alive).length;
  $('#metric-running').textContent = state.streams.filter(stream => stream.running).length;
  $('#last-refresh').textContent = `Изменено ${new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit', second: '2-digit' })}`;
}

function replaceStreamInState(stream) {
  if (!stream?.name) return;
  const index = state.streams.findIndex(item => item.name === stream.name);
  if (index >= 0) state.streams[index] = stream;
  else state.streams.push(stream);
  state.streams.sort((a, b) => (a.position || 0) - (b.position || 0) || a.name.localeCompare(b.name));
  refreshStreamMetrics();
  renderStreams();
}

function successfulResultForSelectedServer(result) {
  return (result?.results || []).find(item => item.ok && item.server_id === state.selectedServerId) || null;
}

async function applyMutationResult(result, streamName, fallbackStream = null) {
  const selectedResult = successfulResultForSelectedServer(result);
  if (!selectedResult) return false;
  const normalized = selectedResult.data?.normalized_stream;
  if (normalized?.name) {
    replaceStreamInState(normalized);
    return true;
  }
  if (fallbackStream?.name) {
    replaceStreamInState(fallbackStream);
    return true;
  }
  try {
    const detail = await api(`/api/streams/${encodeURIComponent(streamName)}?server_id=${encodeURIComponent(state.selectedServerId)}`);
    replaceStreamInState(detail.stream);
    return true;
  } catch (_) {
    return false;
  }
}

function checkedTargets(container) {
  return $$('input[type="checkbox"]:checked', container).map(input => input.value);
}

function renderTargets(container, checkedIds = null) {
  const available = state.servers.filter(server => server.enabled);
  const ids = checkedIds || available.filter(server => server.online).map(server => server.id);
  container.innerHTML = available.map(server => `
    <label class="target-chip">
      <input type="checkbox" value="${escapeHtml(server.id)}" ${ids.includes(server.id) ? 'checked' : ''}>
      <span>${escapeHtml(server.name)}${server.primary ? ' · основной' : ''}${server.online ? '' : ' · offline'}</span>
    </label>`).join('') || '<span class="muted">Нет включённых серверов</span>';
}

function streamPlacement(stream) {
  return stream?.placement || { enabled: state.placementEnabled, configured_mode: 'mirror', effective_mode: 'mirror', primary_server_id: null };
}

function desiredTargetIds(stream = null) {
  const placement = streamPlacement(stream);
  if (state.placementEnabled && placement.configured_mode === 'assigned' && placement.primary_server_id) return [placement.primary_server_id];
  return state.servers.filter(server => server.enabled && server.online).map(server => server.id);
}

function fillPlacementServerSelect(select, selected = '') {
  const counts = new Map((state.placementServerCounts || []).map(item => [item.server_id, item]));
  select.innerHTML = '<option value="">Выберите сервер</option>' + state.servers.filter(server => server.enabled).map(server => {
    const capacity=counts.get(server.id); const suffix=capacity ? ` · ${capacity.assigned}/${capacity.capacity}` : '';
    return `<option value="${escapeHtml(server.id)}" ${server.id === selected ? 'selected' : ''}>${escapeHtml(server.name)}${escapeHtml(suffix)}${server.online ? '' : ' · offline'}</option>`;
  }).join('');
}

function addInputRow(container, value = '', draggable = true) {
  const row = document.createElement('div');
  row.className = 'input-row';
  row.draggable = draggable;
  row.innerHTML = `
    <span class="input-priority"></span>
    <span class="drag-handle" title="Перетащить">⋮⋮</span>
    <input class="input-url" placeholder="hls://, m4f://, http://, udp:// …" value="${escapeHtml(value)}" required>
    <button class="remove-input" type="button" title="Удалить">×</button>`;
  $('.remove-input', row).addEventListener('click', () => {
    if ($$('.input-row', container).length <= 1) return toast('У потока должен остаться хотя бы один input', 'error');
    row.remove();
    renumberInputs(container);
  });
  if (draggable) wireInputDrag(row, container);
  container.append(row);
  renumberInputs(container);
}

function renumberInputs(container) {
  $$('.input-row', container).forEach((row, index) => $('.input-priority', row).textContent = index + 1);
}

function wireInputDrag(row, container) {
  row.addEventListener('dragstart', () => row.classList.add('dragging'));
  row.addEventListener('dragend', () => {
    row.classList.remove('dragging');
    $$('.input-row', container).forEach(item => item.classList.remove('drag-over'));
    renumberInputs(container);
  });
  row.addEventListener('dragover', event => {
    event.preventDefault();
    const dragging = $('.input-row.dragging', container);
    if (!dragging || dragging === row) return;
    row.classList.add('drag-over');
  });
  row.addEventListener('dragleave', () => row.classList.remove('drag-over'));
  row.addEventListener('drop', event => {
    event.preventDefault();
    const dragging = $('.input-row.dragging', container);
    if (!dragging || dragging === row) return;
    const rect = row.getBoundingClientRect();
    container.insertBefore(dragging, event.clientY < rect.top + rect.height / 2 ? row : row.nextSibling);
    row.classList.remove('drag-over');
    renumberInputs(container);
  });
}

function inputValues(container) {
  return $$('.input-url', container).map(input => input.value.trim()).filter(Boolean);
}

async function init() {
  try {
    const me = await api('/api/auth/me');
    state.user = me.username;
    $('#profile-user').textContent = me.username;
    document.title = me.title;
    showApp();
    await loadServers();
    await loadPlacementSettings();
    await loadStreams();
  } catch (_) {
    showLogin();
  }
}

async function loadServers() {
  try {
    const data = await api('/api/servers');
    state.servers = data.items;
    state.primaryId = data.primary_id;
    const enabledIds = state.servers.filter(server => server.enabled).map(server => server.id);
    if (!state.selectedServerId || !enabledIds.includes(state.selectedServerId)) {
      state.selectedServerId = state.primaryId || enabledIds[0] || null;
    }
    renderServerSelect();
    renderServerCards();
    const enabled = state.servers.filter(server => server.enabled);
    const online = enabled.filter(server => server.online).length;
    $('#metric-servers').textContent = `${online}/${enabled.length}`;
    $('#metric-servers-foot').textContent = enabled.length === 0 ? 'добавьте сервер' : (online === enabled.length ? 'все доступны' : 'есть недоступные');
  } catch (error) {
    toast(`Ошибка проверки серверов: ${error.message}`, 'error');
  }
}

function renderServerSelect() {
  const enabled = state.servers.filter(server => server.enabled);
  $('#server-select').disabled = enabled.length === 0;
  $('#server-select').innerHTML = enabled.length ? enabled.map(server => `
    <option value="${escapeHtml(server.id)}" ${server.id === state.selectedServerId ? 'selected' : ''}>
      ${escapeHtml(server.name)}${server.primary ? ' · основной' : ''}${server.online ? '' : ' · offline'}
    </option>`).join('') : '<option value="">Нет серверов</option>';
}

function renderServerCards() {
  const grid = $('#server-grid');
  const empty = $('#server-empty');
  empty.classList.toggle('hidden', state.servers.length > 0);
  grid.classList.toggle('hidden', state.servers.length === 0);
  grid.innerHTML = state.servers.map(server => `
    <article class="server-card ${server.enabled ? '' : 'disabled-server'}">
      <div class="server-card-head">
        <div><div class="server-title-line"><h3>${escapeHtml(server.name)}</h3>${server.primary ? '<span class="primary-badge">Основной</span>' : ''}</div><div class="server-url">${escapeHtml(server.url)}</div><div class="server-user">API: ${escapeHtml(server.username)}</div><div class="server-user">Node Exporter: ${escapeHtml(server.node_exporter_url || 'авто · HOST:9100/metrics')}</div></div>
        <span class="status-pill ${server.online && server.enabled ? 'alive' : ''}">${server.enabled ? (server.online ? 'Online' : 'Offline') : 'Отключён'}</span>
      </div>
      <div class="server-status">
        <div class="server-stat"><span>ЗАДЕРЖКА API</span><strong>${server.latency_ms ?? '—'} ms</strong></div>
        <div class="server-stat"><span>ПОТОКИ</span><strong>${server.stream_count_hint ?? '—'}</strong></div>
        <div class="server-stat"><span>TLS</span><strong>${server.verify_tls ? 'Проверяется' : 'Без проверки'}</strong></div>
      </div>
      ${server.error ? `<p class="form-error">${escapeHtml(server.error)}</p>` : ''}
      <div class="server-actions">
        <button class="btn ghost small test-saved-server" data-id="${escapeHtml(server.id)}">Проверить</button>
        ${!server.primary && server.enabled ? `<button class="btn ghost small primary-server" data-id="${escapeHtml(server.id)}">Сделать основным</button>` : ''}
        <span class="server-action-spacer"></span>
        <button class="row-action edit-server" data-id="${escapeHtml(server.id)}" title="Редактировать">✎</button>
        <button class="row-action delete delete-server" data-id="${escapeHtml(server.id)}" title="Удалить">⌫</button>
      </div>
    </article>`).join('');

  $$('.edit-server', grid).forEach(button => button.addEventListener('click', () => openServerModal(button.dataset.id)));
  $$('.delete-server', grid).forEach(button => button.addEventListener('click', () => deleteServer(button.dataset.id)));
  $$('.primary-server', grid).forEach(button => button.addEventListener('click', () => setPrimaryServer(button.dataset.id)));
  $$('.test-saved-server', grid).forEach(button => button.addEventListener('click', () => testSavedServer(button.dataset.id, button)));
}

async function loadStreams() {
  state.selectedStreams.clear(); updateBulkState();
  const body = $('#streams-body');
  if (!state.selectedServerId) {
    state.streams = [];
    $('#metric-total').textContent = '0';
    $('#metric-alive').textContent = '0';
    $('#metric-running').textContent = '0';
    $('#stream-count').textContent = '0 потоков';
    body.innerHTML = '<tr class="loading-row"><td colspan="7">Добавьте и включите Flussonic-сервер в разделе «Серверы».</td></tr>';
    $('#empty-state').classList.add('hidden');
    return;
  }
  body.innerHTML = '<tr class="loading-row"><td colspan="7">Загрузка потоков…</td></tr>';
  $('#empty-state').classList.add('hidden');
  try {
    const data = await api(`/api/streams?server_id=${encodeURIComponent(state.selectedServerId || '')}`);
    state.streams = data.items;
    state.placementEnabled = Boolean(data.placement_enabled);
    renderStreams();
    $('#metric-total').textContent = data.stats.total;
    $('#metric-alive').textContent = data.stats.alive;
    $('#metric-running').textContent = data.stats.running;
    $('#last-refresh').textContent = `Обновлено ${new Date().toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' })}`;
  } catch (error) {
    body.innerHTML = `<tr class="loading-row"><td colspan="7">${escapeHtml(error.message)}</td></tr>`;
    toast(`Не удалось загрузить потоки: ${error.message}`, 'error');
  }
}

function filteredStreams() {
  const query = $('#search-input').value.trim().toLowerCase();
  const filter = $('#status-filter').value;
  return state.streams.filter(stream => {
    const haystack = [stream.name, stream.title, stream.provider, ...(stream.inputs || []).map(item => item.url)].join(' ').toLowerCase();
    const searchMatch = !query || haystack.includes(query);
    const statusMatch = filter === 'all'
      || (filter === 'disabled' && stream.disabled)
      || (filter === 'alive' && !stream.disabled && stream.alive)
      || (filter === 'waiting' && !stream.disabled && stream.status === 'waiting')
      || (filter === 'offline' && !stream.disabled && !stream.alive && stream.status !== 'waiting');
    return searchMatch && statusMatch;
  });
}

function renderStreams() {
  const items = filteredStreams();
  $('#stream-count').textContent = `${items.length} потоков`;
  $('#empty-state').classList.toggle('hidden', items.length > 0);
  $('#streams-body').innerHTML = items.map(stream => {
    const [statusClass, statusLabel] = formatStatus(stream);
    const initial = (stream.title || stream.name || '?').trim().charAt(0).toUpperCase();
    const inputIcons = (stream.inputs || []).slice(0, 3).map((_, i) => `<span class="input-dot">${i + 1}</span>`).join('');
    return `<tr draggable="true" data-name="${escapeHtml(stream.name)}" class="${stream.disabled ? 'stream-disabled-row' : ''}">
      <td><input class="stream-select" type="checkbox" ${state.selectedStreams.has(stream.name) ? 'checked' : ''}></td>
      <td><span class="drag-handle" title="Изменить порядок">⋮⋮</span></td>
      <td><div class="stream-cell"><div class="stream-logo">${escapeHtml(initial)}</div><div class="stream-name"><strong>${escapeHtml(stream.title || stream.name)}</strong><span>${escapeHtml(stream.name)} · ${escapeHtml(stream.provider || 'без provider')} · ${streamPlacement(stream).configured_mode === 'assigned' ? escapeHtml(streamPlacement(stream).primary_server_name || 'CDN не выбран') : 'зеркало'}</span></div></div></td>
      <td><div class="input-stack">${inputIcons}<span class="input-count">${stream.inputs.length} input</span></div></td>
      <td><span class="status-pill ${statusClass}">${escapeHtml(statusLabel)}</span></td>
      <td><div class="stream-mode-switch" aria-label="Режим потока">
        <button class="mode-btn ondemand-mode ${stream.static ? '' : 'active'}" title="Запускать по запросу">On demand</button>
        <button class="mode-btn static-mode ${stream.static ? 'active' : ''}" title="Держать поток постоянно включённым">Static</button>
      </div></td>
      <td><span class="mono">${stream.position}</span></td>
      <td><div class="row-actions">
        <button class="row-action inputs-action" title="Редактировать input">⇄</button>
        <button class="row-action compare-action" title="Сравнить серверы">◎</button>
        <button class="row-action preview-action" title="Просмотр">▶</button>
        <button class="row-action diagnostics-action" title="Диагностика">⚙</button>
        <button class="row-action state-action ${stream.disabled ? 'enable-stream-action' : 'disable-stream-action'}" title="${stream.disabled ? 'Включить поток' : 'Временно отключить поток'}">⏻</button>
        <button class="row-action edit-action" title="Редактировать поток">✎</button>
        <button class="row-action delete delete-action" title="Удалить">⌫</button>
      </div></td>
    </tr>`;
  }).join('');
  wireStreamRows();
  updateBulkState();
}

function wireStreamRows() {
  $$('#streams-body tr').forEach(row => {
    const name = row.dataset.name;
    $('.stream-select', row)?.addEventListener('change', event => { event.target.checked ? state.selectedStreams.add(name) : state.selectedStreams.delete(name); updateBulkState(); });
    $('.inputs-action', row)?.addEventListener('click', () => openInputs(name));
    $('.compare-action', row)?.addEventListener('click', () => openCompare(name));
    $('.preview-action', row)?.addEventListener('click', () => openPreview(name));
    $('.diagnostics-action', row)?.addEventListener('click', () => openDiagnostics(name));
    $('.ondemand-mode', row)?.addEventListener('click', event => setStreamsMode([name], false, event.currentTarget));
    $('.static-mode', row)?.addEventListener('click', event => setStreamsMode([name], true, event.currentTarget));
    $('.state-action', row)?.addEventListener('click', event => {
      const stream = state.streams.find(item => item.name === name);
      setStreamsDisabled([name], !Boolean(stream?.disabled), event.currentTarget);
    });
    $('.edit-action', row)?.addEventListener('click', () => openStreamModal(name));
    $('.delete-action', row)?.addEventListener('click', () => openDelete(name));
    row.addEventListener('dragstart', event => {
      if (event.target.closest('button, input, select, a')) return event.preventDefault();
      state.draggedRow = row;
      row.classList.add('dragging');
    });
    row.addEventListener('dragend', () => {
      row.classList.remove('dragging');
      $$('#streams-body tr').forEach(item => item.classList.remove('drag-over'));
      state.draggedRow = null;
    });
    row.addEventListener('dragover', event => {
      event.preventDefault();
      if (state.draggedRow && state.draggedRow !== row) row.classList.add('drag-over');
    });
    row.addEventListener('dragleave', () => row.classList.remove('drag-over'));
    row.addEventListener('drop', async event => {
      event.preventDefault();
      if (!state.draggedRow || state.draggedRow === row) return;
      const body = $('#streams-body');
      const rect = row.getBoundingClientRect();
      body.insertBefore(state.draggedRow, event.clientY < rect.top + rect.height / 2 ? row : row.nextSibling);
      row.classList.remove('drag-over');
      await saveStreamOrder();
    });
  });
}

async function saveStreamOrder() {
  const rows = $$('#streams-body tr[data-name]');
  const visibleNames = rows.map(row => row.dataset.name);
  if (visibleNames.length !== state.streams.length) return toast('Для изменения порядка сбросьте поиск и фильтры', 'error');
  const payload = {
    streams: visibleNames.map((name, index) => ({ name, position: index + 1 })),
    target_ids: [state.selectedServerId],
  };
  try {
    const result = await api('/api/streams/reorder/batch', { method: 'PUT', body: JSON.stringify(payload) });
    reportOperation(result, 'Порядок потоков обновлён');
    if ((result.results || []).some(item => item.ok && item.server_id === state.selectedServerId)) {
      const positions = new Map(payload.streams.map(item => [item.name, item.position]));
      state.streams = state.streams.map(stream => ({ ...stream, position: positions.get(stream.name) ?? stream.position }));
      state.streams.sort((a, b) => (a.position || 0) - (b.position || 0) || a.name.localeCompare(b.name));
      refreshStreamMetrics();
      renderStreams();
    }
  } catch (error) {
    toast(`Не удалось изменить порядок: ${error.message}`, 'error');
    await loadStreams();
  }
}

function updateM3UPlacementFields() {
  const assigned = $('#m3u-placement-mode').value === 'assigned';
  $('#m3u-placement-server-field').classList.toggle('hidden', !assigned);
}

function openM3UImport() {
  state.m3uPreview = null;
  $('#m3u-import-text').value = '';
  $('#m3u-placement-mode').value = 'mirror';
  $('#m3u-overwrite-existing').checked = false;
  $('#m3u-stream-mode').value = 'ondemand';
  fillPlacementServerSelect($('#m3u-placement-server'));
  $('#m3u-preview-summary').textContent = 'Ещё не проверено';
  $('#m3u-preview-body').innerHTML = '<tr><td colspan="4" class="muted">Вставьте M3U и нажмите «Предпросмотр»</td></tr>';
  $('#m3u-preview-warnings').textContent = '';
  updateM3UPlacementFields();
  $('#m3u-import-dialog').showModal();
}

function m3uImportPayload() {
  const content = $('#m3u-import-text').value.trim();
  const placementMode = $('#m3u-placement-mode').value;
  const placementServerId = $('#m3u-placement-server').value;
  if (!content) throw new Error('Вставьте M3U список');
  if (placementMode === 'assigned' && !placementServerId) throw new Error('Выберите основной CDN');
  return {
    content,
    placement_mode: placementMode,
    placement_server_id: placementMode === 'assigned' ? placementServerId : null,
    provider: 'CYRIUSTV',
    on_play: 'auth://NewAuthBackend1',
    static: $('#m3u-stream-mode').value === 'static',
    overwrite_existing: $('#m3u-overwrite-existing').checked,
  };
}

function renderM3UPreview(data) {
  state.m3uPreview = data;
  const items = data.items || [];
  const existing = items.filter(item => item.existing).length;
  $('#m3u-preview-summary').textContent = `${items.length} каналов${existing ? ` · ${existing} уже есть` : ''}`;
  const visible = items.slice(0, 250);
  $('#m3u-preview-body').innerHTML = visible.map(item => `<tr>
    <td><strong>${escapeHtml(item.title)}</strong></td>
    <td class="mono">${escapeHtml(item.name)}</td>
    <td>${escapeHtml(item.url)}</td>
    <td><span class="status-pill ${item.existing ? 'waiting' : 'alive'}">${item.existing ? 'уже существует' : 'новый'}</span></td>
  </tr>`).join('') || '<tr><td colspan="4">Каналы не найдены</td></tr>';
  const messages = [...(data.warnings || [])];
  if (items.length > visible.length) messages.push(`В таблице показаны первые ${visible.length} из ${items.length} каналов.`);
  for (const error of (data.inventory_errors || [])) messages.push(`${error.server_name}: не удалось проверить существующие потоки — ${error.error}`);
  $('#m3u-preview-warnings').textContent = messages.join('\n');
}

async function previewM3UImport() {
  const button = $('#m3u-preview-btn');
  let payload;
  try { payload = m3uImportPayload(); } catch (error) { return toast(error.message, 'error'); }
  setBusy(button, true, 'Проверка…');
  try {
    const data = await api('/api/import/m3u/preview', { method: 'POST', body: JSON.stringify(payload) });
    renderM3UPreview(data);
  } catch (error) { toast(error.message, 'error', 6500); }
  finally { setBusy(button, false); }
}

async function applyM3UImport(event) {
  event.preventDefault();
  const button = $('#m3u-import-submit');
  let payload;
  try { payload = m3uImportPayload(); } catch (error) { return toast(error.message, 'error'); }
  if (payload.overwrite_existing && !window.confirm('Включено обновление существующих потоков. Продолжить массовый импорт?')) return;
  setBusy(button, true, 'Импорт…');
  try {
    const result = await api('/api/import/m3u/apply', { method: 'POST', body: JSON.stringify(payload) });
    const message = `M3U: создано ${result.created}, пропущено ${result.skipped}, ошибок ${result.failed}`;
    toast(message, result.failed ? 'error' : 'success', 7500);
    if (result.created || result.skipped) $('#m3u-import-dialog').close();
    await loadStreams();
    if (state.placementEnabled) await loadPlacementOverview();
  } catch (error) { toast(error.message, 'error', 7000); }
  finally { setBusy(button, false); }
}

function openStreamModal(name = null) {
  state.currentStream = name ? state.streams.find(item => item.name === name) : null;
  const isEdit = Boolean(state.currentStream);
  $('#stream-modal-title').textContent = isEdit ? 'Редактирование потока' : 'Новый поток';
  $('#stream-submit').textContent = isEdit ? 'Сохранить изменения' : 'Создать поток';
  $('#stream-name').disabled = isEdit;
  $('#stream-name').value = state.currentStream?.name || '';
  $('#stream-title').value = state.currentStream?.title || '';
  $('#stream-provider').value = state.currentStream?.provider || 'CYRIUSTV';
  $('#stream-on-play').value = state.currentStream?.on_play || 'auth://NewAuthBackend1';
  $('#stream-position').value = state.currentStream?.position ?? '';
  $('#stream-static').checked = Boolean(state.currentStream?.static);
  const list = $('#create-input-list');
  list.innerHTML = '';
  (state.currentStream?.inputs?.length ? state.currentStream.inputs : [{ url: '' }]).forEach(item => addInputRow(list, item.url, true));
  const placement = streamPlacement(state.currentStream);
  $('#stream-placement-mode').value = placement.configured_mode || 'mirror';
  fillPlacementServerSelect($('#stream-placement-server'), placement.primary_server_id || '');
  renderTargets($('#create-targets'), desiredTargetIds(state.currentStream));
  updateStreamPlacementFields();
  $('#stream-dialog').showModal();
}

async function saveStream(event) {
  event.preventDefault();
  const button = $('#stream-submit');
  const urls = inputValues($('#create-input-list'));
  if (!urls.length) return toast('Добавьте хотя бы один input', 'error');
  if (new Set(urls).size !== urls.length) return toast('Одинаковый input указан дважды', 'error');
  let targetIds = checkedTargets($('#create-targets'));
  const placementMode = $('#stream-placement-mode').value;
  const placementServerId = $('#stream-placement-server').value;
  if (placementMode === 'assigned') {
    if (!placementServerId) return toast('Выберите основной CDN', 'error');
    targetIds = [placementServerId];
  }
  if (!targetIds.length) return toast('Выберите хотя бы один сервер', 'error');
  setBusy(button, true);
  try {
    const common = {
      title: $('#stream-title').value.trim(),
      provider: $('#stream-provider').value.trim(),
      on_play: $('#stream-on-play').value.trim(),
      static: $('#stream-static').checked,
      position: $('#stream-position').value ? Number($('#stream-position').value) : (state.currentStream?.position ?? null),
      inputs: urls.map(url => ({ url })),
      target_ids: targetIds,
      placement_mode: $('#stream-placement-mode').value,
      placement_server_id: $('#stream-placement-mode').value === 'assigned' ? $('#stream-placement-server').value : null,
    };

    if (state.currentStream) {
      const fallback = {
        ...state.currentStream,
        ...common,
        inputs: common.inputs,
        on_play: common.on_play || null,
        position: common.position ?? state.currentStream.position,
      };
      delete fallback.target_ids;
      delete fallback.placement_mode;
      delete fallback.placement_server_id;
      fallback.placement = { enabled: state.placementEnabled, configured_mode: common.placement_mode, effective_mode: state.placementEnabled ? common.placement_mode : 'mirror', primary_server_id: common.placement_server_id, primary_server_name: state.servers.find(server => server.id === common.placement_server_id)?.name || null };
      const result = await api(`/api/streams/${encodeURIComponent(state.currentStream.name)}`, {
        method: 'PATCH',
        body: JSON.stringify(common),
      });
      reportOperation(result, 'Поток сохранён');
      await applyMutationResult(result, state.currentStream.name, fallback);
    } else {
      const payload = { name: $('#stream-name').value.trim(), ...common };
      const result = await api('/api/streams', { method: 'POST', body: JSON.stringify(payload) });
      reportOperation(result, 'Поток создан');
      const fallback = {
        name: payload.name,
        title: payload.title,
        provider: payload.provider,
        on_play: payload.on_play || null,
        static: payload.static,
        position: payload.position ?? Math.max(0, ...state.streams.map(item => item.position || 0)) + 1,
        inputs: payload.inputs,
        status: 'waiting',
        running: false,
        alive: false,
        named_by: 'config',
        placement: { enabled: state.placementEnabled, configured_mode: payload.placement_mode, effective_mode: state.placementEnabled ? payload.placement_mode : 'mirror', primary_server_id: payload.placement_server_id, primary_server_name: state.servers.find(server => server.id === payload.placement_server_id)?.name || null },
      };
      await applyMutationResult(result, payload.name, fallback);
    }
    $('#stream-dialog').close();
  } catch (error) {
    toast(error.message, 'error', 6000);
  } finally {
    setBusy(button, false);
  }
}

function openInputs(name) {
  const stream = state.streams.find(item => item.name === name);
  if (!stream) return;
  state.currentStream = stream;
  $('#inputs-modal-title').textContent = stream.title || stream.name;
  $('#inputs-stream-name').textContent = stream.name;
  const list = $('#edit-input-list');
  list.innerHTML = '';
  stream.inputs.forEach(item => addInputRow(list, item.url, true));
  renderTargets($('#edit-targets'), desiredTargetIds(stream));
  $('#inputs-dialog').showModal();
}

async function saveInputs(event) {
  event.preventDefault();
  const button = $('#inputs-form button[type="submit"]');
  const urls = inputValues($('#edit-input-list'));
  const targets = checkedTargets($('#edit-targets'));
  if (!urls.length) return toast('Добавьте хотя бы один input', 'error');
  if (new Set(urls).size !== urls.length) return toast('Одинаковый input указан дважды', 'error');
  if (!targets.length) return toast('Выберите хотя бы один сервер', 'error');
  setBusy(button, true);
  try {
    const result = await api(`/api/streams/${encodeURIComponent(state.currentStream.name)}/inputs`, {
      method: 'PUT', body: JSON.stringify({ inputs: urls.map(url => ({ url })), target_ids: targets }),
    });
    reportOperation(result, 'Источники обновлены');
    const fallback = { ...state.currentStream, inputs: urls.map(url => ({ url })) };
    await applyMutationResult(result, state.currentStream.name, fallback);
    $('#inputs-dialog').close();
  } catch (error) {
    toast(error.message, 'error', 6000);
  } finally { setBusy(button, false); }
}

async function openCompare(name) {
  state.compareStream = name;
  $('#compare-title').textContent = `Сравнение: ${name}`;
  $('#compare-summary').className = 'sync-banner';
  $('#compare-summary').textContent = 'Сравниваем конфигурацию на серверах…';
  $('#compare-list').innerHTML = '';
  $('#compare-dialog').showModal();
  try {
    const data = await api(`/api/compare/${encodeURIComponent(name)}`);
    $('#compare-summary').className = `sync-banner ${data.in_sync ? 'ok' : 'warn'}`;
    const model=data.placement?.effective_mode === 'assigned' ? `назначенный CDN: ${data.placement.primary_server_name || 'не найден'}` : 'зеркальная модель';
    $('#compare-summary').textContent = data.in_sync ? `Размещение соответствует модели (${model}).` : `Обнаружены отклонения (${model}).`;
    $('#compare-list').innerHTML = data.items.map(item => { const [cls,label]=syncStateLabel(item); return `
      <div class="compare-row">
        <div><strong>${escapeHtml(item.server_name)}</strong><span>${item.present ? `${item.config?.inputs?.length || 0} input · ${item.expected ? 'требуется' : 'не назначен'}` : escapeHtml(item.error || (item.expected ? 'поток отсутствует' : 'не требуется'))}</span></div>
        <span class="status-pill ${cls === 'ok' ? 'alive' : cls === 'warn' ? 'waiting' : ''}">${escapeHtml(label)}</span>
        <span class="hash">${item.hash || '—'}</span>
      </div>`; }).join('');
  } catch (error) {
    $('#compare-summary').className = 'sync-banner warn';
    $('#compare-summary').textContent = error.message;
  }
}

async function syncCurrentStream() {
  if (!state.compareStream) return;
  const button = $('#sync-stream-btn');
  setBusy(button, true, 'Синхронизация…');
  try {
    const result = await api(`/api/sync/${encodeURIComponent(state.compareStream)}`, {
      method: 'POST', body: JSON.stringify({ source_id: state.primaryId }),
    });
    reportOperation(result, 'Поток синхронизирован');
    await openCompare(state.compareStream);
    await loadStreams();
  } catch (error) { toast(error.message, 'error', 6000); }
  finally { setBusy(button, false); }
}

function openDelete(name) {
  state.currentStream = state.streams.find(item => item.name === name);
  $('#confirm-text').innerHTML = `Поток <strong>${escapeHtml(name)}</strong> будет удалён с выбранных серверов. Это действие нельзя отменить.`;
  renderTargets($('#delete-targets'));
  $('#confirm-dialog').showModal();
}

async function deleteCurrentStream() {
  const targets = checkedTargets($('#delete-targets'));
  if (!targets.length) return toast('Выберите хотя бы один сервер', 'error');
  const button = $('#confirm-delete-btn');
  setBusy(button, true, 'Удаление…');
  try {
    const query = targets.map(id => `target_id=${encodeURIComponent(id)}`).join('&');
    const result = await api(`/api/streams/${encodeURIComponent(state.currentStream.name)}?${query}`, { method: 'DELETE' });
    reportOperation(result, 'Поток удалён');
    if ((result.results || []).some(item => item.ok && item.server_id === state.selectedServerId)) {
      state.streams = state.streams.filter(item => item.name !== state.currentStream.name);
      refreshStreamMetrics();
      renderStreams();
    }
    $('#confirm-dialog').close();
  } catch (error) { toast(error.message, 'error', 6000); }
  finally { setBusy(button, false); }
}

function reportOperation(result, successMessage) {
  const timing = result.max_elapsed_ms ? ` за ${(result.max_elapsed_ms / 1000).toFixed(result.max_elapsed_ms >= 1000 ? 1 : 2)} с` : '';
  if (result.ok) return toast(`${successMessage}: ${result.success_count} сервер(а)${timing}`);
  if (result.partial) return toast(`${successMessage} частично: успешно ${result.success_count}, ошибок ${result.failure_count}${timing}`, 'error', 6500);
  const errors = (result.results || []).filter(item => !item.ok).map(item => `${item.server_name}: ${item.error}`).join('; ');
  toast(errors || 'Операция не выполнена', 'error', 7000);
}


function formatMonitorTime(ts) {
  if (!ts) return '—';
  return new Date(ts * 1000).toLocaleString('ru-RU');
}

function monitorMatches(row, fields) {
  const query = ($('#monitor-filter')?.value || '').trim().toLocaleLowerCase('ru-RU');
  if (!query) return true;
  return fields.some(field => String(row[field] ?? '').toLocaleLowerCase('ru-RU').includes(query));
}

async function loadMonitorSnapshot(refresh = false) {
  try {
    const payload = await api(`/api/monitor/snapshot${refresh ? '?refresh=true' : ''}`);
    renderMonitor(payload, state.monitorRange === 'live');
  } catch (error) {
    toast(`Ошибка мониторинга: ${error.message}`, 'error', 6500);
    setMonitorConnection(false, 'Ошибка API');
  }
}

function startMonitor() {
  if (!state.monitorPayload) loadMonitorSnapshot();
  if (state.monitorEventSource) {
    setTimeout(drawMonitorChart, 0);
    return;
  }
  const source = new EventSource('/api/monitor/stream');
  state.monitorEventSource = source;
  setMonitorConnection(false, 'Подключение…');
  source.onopen = () => setMonitorConnection(true, 'LIVE');
  source.onmessage = event => {
    try {
      renderMonitor(JSON.parse(event.data), state.monitorRange === 'live');
      setMonitorConnection(true, 'LIVE');
    } catch (error) {
      console.error('Monitor payload error', error);
    }
  };
  source.onerror = () => setMonitorConnection(false, 'Переподключение…');
}

function stopMonitor() {
  if (state.monitorEventSource) {
    state.monitorEventSource.close();
    state.monitorEventSource = null;
  }
  setMonitorConnection(false, 'Отключено');
}

function setMonitorConnection(online, label) {
  const badge = $('#monitor-connection');
  const dot = $('#monitor-live-dot');
  if (!badge || !dot) return;
  badge.textContent = label;
  badge.classList.toggle('monitor-online', online);
  dot.classList.toggle('online', online);
}

function pushMonitorHistory(payload) {
  const ts = payload.ts || Math.floor(Date.now() / 1000);
  if (state.monitorHistory.length && state.monitorHistory.at(-1).ts === ts) return;
  const counts = {};
  for (const item of payload.server_counts || []) counts[item.server_id] = item.sessions || 0;
  state.monitorHistory.push({ ts, total: payload.metrics?.sessions || 0, counts });
  while (state.monitorHistory.length > 300) state.monitorHistory.shift();
}

function drawMonitorChart() {
  const canvas = $('#monitor-chart');
  if (!canvas || canvas.offsetParent === null) return;
  const rect = canvas.getBoundingClientRect();
  if (!rect.width || !rect.height) return;
  const dpr = Math.max(1, window.devicePixelRatio || 1);
  canvas.width = Math.round(rect.width * dpr);
  canvas.height = Math.round(rect.height * dpr);
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  const width = rect.width;
  const height = rect.height;
  ctx.clearRect(0, 0, width, height);

  const history = state.monitorHistory;
  if (history.length < 2) {
    ctx.fillStyle = '#7f8ba3';
    ctx.font = '12px system-ui';
    ctx.fillText('График появится после нескольких циклов опроса', 18, height / 2);
    return;
  }

  const palette = ['#23d5e6', '#7c5cff', '#2dd4a7', '#f8b84e', '#ff6577', '#72a7ff', '#c880ff', '#8ad46f'];
  const serverItems = state.monitorPayload?.server_counts || [];
  const ids = serverItems.map(item => item.server_id);
  let maxValue = 1;
  for (const point of history) {
    maxValue = Math.max(maxValue, point.total, ...Object.values(point.counts));
  }

  const pad = { left: 38, right: 16, top: 18, bottom: 25 };
  const chartWidth = width - pad.left - pad.right;
  const chartHeight = height - pad.top - pad.bottom;
  const x = index => pad.left + chartWidth * (index / Math.max(1, history.length - 1));
  const y = value => pad.top + chartHeight * (1 - value / maxValue);

  ctx.strokeStyle = 'rgba(153, 166, 194, .14)';
  ctx.lineWidth = 1;
  ctx.fillStyle = '#6e7a92';
  ctx.font = '10px system-ui';
  for (let index = 0; index <= 4; index += 1) {
    const value = Math.round(maxValue * (1 - index / 4));
    const lineY = pad.top + chartHeight * (index / 4);
    ctx.beginPath();
    ctx.moveTo(pad.left, lineY);
    ctx.lineTo(width - pad.right, lineY);
    ctx.stroke();
    ctx.fillText(String(value), 6, lineY + 3);
  }

  function drawLine(values, color, lineWidth) {
    ctx.beginPath();
    values.forEach((value, index) => {
      const pointX = x(index);
      const pointY = y(value);
      if (index === 0) ctx.moveTo(pointX, pointY);
      else ctx.lineTo(pointX, pointY);
    });
    ctx.strokeStyle = color;
    ctx.lineWidth = lineWidth;
    ctx.lineJoin = 'round';
    ctx.lineCap = 'round';
    ctx.stroke();
  }

  drawLine(history.map(point => point.total), '#f3f6ff', 2.4);
  ids.forEach((id, index) => drawLine(history.map(point => point.counts[id] || 0), palette[index % palette.length], 1.7));

  const first = new Date(history[0].ts * 1000).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
  const last = new Date(history.at(-1).ts * 1000).toLocaleTimeString('ru-RU', { hour: '2-digit', minute: '2-digit' });
  ctx.fillStyle = '#6e7a92';
  ctx.fillText(first, pad.left, height - 6);
  const lastWidth = ctx.measureText(last).width;
  ctx.fillText(last, width - pad.right - lastWidth, height - 6);

  const legend = $('#monitor-legend');
  if (legend) {
    legend.innerHTML = `<span><i style="background:#f3f6ff"></i>Всего</span>` + serverItems.map((item, index) =>
      `<span><i style="background:${palette[index % palette.length]}"></i>${escapeHtml(item.server)} · ${item.sessions}</span>`
    ).join('');
  }
}

function renderMonitor(payload, addHistory = false) {
  state.monitorPayload = payload;
  if (addHistory) pushMonitorHistory(payload);
  const metrics = payload.metrics || {};
  $('#monitor-metric-sessions').textContent = metrics.sessions ?? 0;
  $('#monitor-metric-logins').textContent = metrics.logins ?? 0;
  $('#monitor-metric-ips').textContent = metrics.unique_ips ?? 0;
  $('#monitor-metric-streams').textContent = metrics.active_streams ?? 0;
  $('#monitor-poll-seconds').textContent = `${payload.poll_seconds ?? '—'} сек`;
  $('#last-refresh').textContent = `Обновлено ${formatMonitorTime(payload.ts)}`;

  const serverSummary = (payload.server_counts || []).map(item => `${item.server}: ${item.sessions}`).join(' · ');
  if (state.monitorRange === 'live') $('#monitor-chart-info').textContent = serverSummary || 'Нет включённых серверов';

  const errorBox = $('#monitor-errors');
  const errors = payload.errors || [];
  errorBox.classList.toggle('hidden', errors.length === 0);
  errorBox.innerHTML = errors.map(error => `<div>${escapeHtml(error)}</div>`).join('');

  const totals = (payload.totals || []).filter(row => monitorMatches(row, ['login', 'servers']));
  $('#monitor-totals-count').textContent = totals.length;
  $('#monitor-totals-body').innerHTML = totals.map(row => `
    <tr><td><strong>${escapeHtml(row.login)}</strong></td><td>${row.sessions}</td><td>${row.unique_ips}</td><td>${escapeHtml(row.servers)}</td><td>${row.server_count}</td></tr>`
  ).join('') || monitorEmptyRow(5);

  const perServer = (payload.per_server || []).filter(row => monitorMatches(row, ['login', 'server', 'ips']));
  $('#monitor-per-count').textContent = perServer.length;
  $('#monitor-per-body').innerHTML = perServer.map(row => `
    <tr><td><strong>${escapeHtml(row.login)}</strong></td><td>${escapeHtml(row.server)}</td><td>${row.sessions}</td><td>${row.unique_ips}</td><td class="monitor-ip-list">${escapeHtml(row.ips)}</td></tr>`
  ).join('') || monitorEmptyRow(5);

  const uptime = (payload.streams_uptime || []).filter(row => monitorMatches(row, ['channel', 'server']));
  $('#monitor-uptime-count').textContent = uptime.length;
  $('#monitor-uptime-body').innerHTML = uptime.map(row => `
    <tr><td>${escapeHtml(row.server)}</td><td><strong>${escapeHtml(row.channel)}</strong></td><td>${row.sessions}</td><td><span class="uptime-value">${escapeHtml(row.uptime)}</span></td><td class="muted">${formatMonitorTime(row.first_seen)}</td></tr>`
  ).join('') || monitorEmptyRow(5);

  let sessions = (payload.sessions || []).filter(row => monitorMatches(row, ['channel', 'login', 'server', 'ip']));
  sessions = sortMonitorSessions(sessions);
  $('#monitor-sessions-count').textContent = sessions.length;
  $('#monitor-sessions-body').innerHTML = sessions.map(row => `
    <tr><td>${escapeHtml(row.channel)}</td><td><strong>${escapeHtml(row.login)}</strong></td><td>${escapeHtml(row.server)}</td><td class="mono">${escapeHtml(row.ip)}</td></tr>`
  ).join('') || monitorEmptyRow(4);

  requestAnimationFrame(drawMonitorChart);
}


function formatBytes(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—';
  let amount = Number(value);
  const units = ['B', 'KB', 'MB', 'GB', 'TB'];
  let index = 0;
  while (Math.abs(amount) >= 1024 && index < units.length - 1) { amount /= 1024; index += 1; }
  return `${amount.toFixed(index >= 3 ? 1 : 0)} ${units[index]}`;
}

function formatBitrate(value) {
  if (value === null || value === undefined || !Number.isFinite(Number(value))) return '—';
  let amount = Number(value);
  const units = ['бит/с', 'Кбит/с', 'Мбит/с', 'Гбит/с'];
  let index = 0;
  while (Math.abs(amount) >= 1000 && index < units.length - 1) { amount /= 1000; index += 1; }
  return `${amount.toFixed(index >= 2 ? 1 : 0)} ${units[index]}`;
}

function loadPercent(value) {
  return value === null || value === undefined || !Number.isFinite(Number(value)) ? '—' : `${Number(value).toFixed(1)}%`;
}

async function loadLoadSnapshot(refresh = false) {
  try {
    const payload = await api(`/api/load/snapshot${refresh ? '?refresh=true' : ''}`);
    renderLoad(payload, state.loadRange === 'live');
  } catch (error) {
    toast(`Ошибка мониторинга нагрузки: ${error.message}`, 'error', 6500);
    setLoadConnection(false, 'Ошибка API');
  }
}

function startLoadMonitor() {
  if (!state.loadPayload) loadLoadSnapshot();
  if (state.loadEventSource) {
    setTimeout(drawLoadChart, 0);
    return;
  }
  const source = new EventSource('/api/load/stream');
  state.loadEventSource = source;
  setLoadConnection(false, 'Подключение…');
  source.onopen = () => setLoadConnection(true, 'LIVE');
  source.onmessage = event => {
    try {
      renderLoad(JSON.parse(event.data), state.loadRange === 'live');
      setLoadConnection(true, 'LIVE');
    } catch (error) {
      console.error('Load payload error', error);
    }
  };
  source.onerror = () => setLoadConnection(false, 'Переподключение…');
}

function stopLoadMonitor() {
  if (state.loadEventSource) {
    state.loadEventSource.close();
    state.loadEventSource = null;
  }
  setLoadConnection(false, 'Отключено');
}

function setLoadConnection(online, label) {
  const badge = $('#load-connection');
  const dot = $('#load-live-dot');
  if (!badge || !dot) return;
  badge.textContent = label;
  badge.classList.toggle('monitor-online', online);
  dot.classList.toggle('online', online);
}

function pushLoadHistory(payload) {
  const ts = payload.network_ts || payload.ts || Math.floor(Date.now() / 1000);
  for (const item of payload.servers || []) {
    const last = [...state.loadHistory].reverse().find(point => point.server_id === item.server_id);
    if (last?.ts === ts) continue;
    state.loadHistory.push({ ts, ...item });
  }
  const cutoff = ts - 600;
  state.loadHistory = state.loadHistory.filter(point => point.ts >= cutoff);
}

function loadMetricValue(point, metric) {
  if (metric === 'network') return (Number(point.network_rx_bps || 0) + Number(point.network_tx_bps || 0)) / 1_000_000;
  if (metric === 'network_rx_bps') return Number(point.network_rx_bps || 0) / 1_000_000;
  if (metric === 'network_tx_bps') return Number(point.network_tx_bps || 0) / 1_000_000;
  if (metric === 'process_memory_bytes') return Number(point.process_memory_bytes || 0) / (1024 * 1024);
  return point[metric] === null || point[metric] === undefined ? null : Number(point[metric]);
}

function loadMetricLabel(metric) {
  return {
    cpu_percent: 'CPU, %', memory_percent: 'RAM, %', disk_percent: 'Диск, %',
    network: 'Сеть RX+TX, Мбит/с', network_rx_bps: 'Входящий RX, Мбит/с', network_tx_bps: 'Исходящий TX, Мбит/с', process_memory_bytes: 'Память Flussonic, MB', sessions: 'Сессии',
  }[metric] || metric;
}

function updateLoadServerSelect(payload) {
  const select = $('#load-server-select');
  if (!select) return;
  const current = state.loadServerId;
  select.innerHTML = '<option value="all">Все серверы</option>' + (payload.servers || []).map(item =>
    `<option value="${escapeHtml(item.server_id)}">${escapeHtml(item.server)}</option>`
  ).join('');
  state.loadServerId = (payload.servers || []).some(item => item.server_id === current) ? current : 'all';
  select.value = state.loadServerId;
}

function loadBar(label, value, threshold, detail = '') {
  const numeric = value === null || value === undefined ? null : Number(value);
  const width = numeric === null ? 0 : Math.max(0, Math.min(100, numeric));
  const stateClass = numeric === null ? 'unknown' : numeric >= threshold * 1.12 ? 'critical' : numeric >= threshold ? 'warning' : 'ok';
  return `<div class="load-bar-row"><div class="load-bar-head"><span>${label}</span><strong>${loadPercent(numeric)}</strong></div><div class="load-progress"><i class="${stateClass}" style="width:${width}%"></i></div>${detail ? `<small>${escapeHtml(detail)}</small>` : ''}</div>`;
}

function renderLoad(payload, addHistory = false) {
  state.loadPayload = payload;
  if (addHistory) pushLoadHistory(payload);
  updateLoadServerSelect(payload);
  const summary = payload.summary || {};
  $('#load-online').textContent = `${summary.online ?? 0}/${summary.servers ?? 0}`;
  $('#load-average-cpu').textContent = loadPercent(summary.avg_cpu_percent);
  $('#load-average-memory').textContent = loadPercent(summary.avg_memory_percent);
  $('#load-network-rx').textContent = formatBitrate(summary.network_rx_bps || 0);
  $('#load-network-tx').textContent = formatBitrate(summary.network_tx_bps || 0);
  $('#load-warnings').textContent = summary.warnings ?? 0;
  $('#load-network-total').textContent = `RX+TX ${formatBitrate(summary.network_bps || 0)}`;
  $('#load-poll-seconds').textContent = `система ${payload.poll_seconds ?? '—'}с · сеть ${payload.network_poll_seconds ?? '—'}с`;
  const networkAge = payload.network_ts ? Math.max(0, Math.floor(Date.now() / 1000) - Number(payload.network_ts)) : null;
  if ($('#load-network-rx-foot')) $('#load-network-rx-foot').textContent = networkAge === null ? 'ожидание данных' : `обновлено ${networkAge}с назад`;
  if ($('#load-network-tx-foot')) $('#load-network-tx-foot').textContent = networkAge === null ? 'ожидание данных' : `обновлено ${networkAge}с назад`;
  $('#last-refresh').textContent = `Обновлено ${formatMonitorTime(payload.ts)}`;

  const errors = payload.errors || [];
  const errorBox = $('#load-errors');
  errorBox.classList.toggle('hidden', errors.length === 0);
  errorBox.innerHTML = errors.map(error => `<div>${escapeHtml(error)}</div>`).join('');

  const thresholds = payload.thresholds || { cpu: 85, memory: 85, disk: 90 };
  const cards = (payload.servers || []).map(item => {
    const stateLabel = item.state === 'critical' ? 'Критично' : item.state === 'warning' ? 'Внимание' : item.state === 'offline' ? 'Offline' : item.state === 'limited' ? 'Ограниченно' : 'Норма';
    const statusClass = item.state === 'ok' ? 'alive' : item.state === 'warning' || item.state === 'limited' ? 'waiting' : item.state === 'critical' || item.state === 'offline' ? 'danger' : '';
    const source = item.source === 'node_exporter' ? `Node Exporter · ${item.metrics_path || ''}` : item.source === 'runtime_metrics' ? `Runtime metrics · ${item.metrics_path || ''}` : 'Fallback по stats потоков';
    const cpuDetail = item.cpu_source === 'process' ? 'нагрузка процесса Flussonic' : item.cpu_count ? `${item.cpu_count} CPU` : '';
    const memoryDetail = item.memory_total_bytes ? `${formatBytes(item.memory_used_bytes)} из ${formatBytes(item.memory_total_bytes)}` : item.process_memory_bytes ? `процесс: ${formatBytes(item.process_memory_bytes)}` : '';
    const diskDetail = item.disk_total_bytes ? `${formatBytes(item.disk_used_bytes)} из ${formatBytes(item.disk_total_bytes)}` : '';
    const limited = item.limited ? `<div class="load-limited">Полные системные метрики недоступны. ${escapeHtml(item.error || '')}</div>` : '';
    const extra = item.limited
      ? `<div class="load-facts"><div><span>CPU units</span><strong>${Number(item.cpu_units || 0).toFixed(1)}</strong></div><div><span>Потоки</span><strong>${item.alive_streams || 0}/${item.streams || 0}</strong></div><div><span>RAM потоков</span><strong>${formatBytes(item.process_memory_bytes)}</strong></div><div><span>Transcoder overload</span><strong>${item.transcoder_overloaded || 0}</strong></div></div>`
      : `<div class="load-facts"><div><span>RX</span><strong>${formatBitrate(item.network_rx_bps)}</strong></div><div><span>TX</span><strong>${formatBitrate(item.network_tx_bps)}</strong></div><div><span>Load avg</span><strong>${item.load1 === null || item.load1 === undefined ? '—' : `${Number(item.load1).toFixed(2)} / ${Number(item.load5 || 0).toFixed(2)}`}</strong></div><div><span>Интерфейсы</span><strong>${(item.network_interfaces || []).length}</strong></div></div>`;
    const nodeLabel = item.source === 'node_exporter' ? 'LINUX · NODE EXPORTER' : item.online ? 'FLUSSONIC NODE' : 'CONNECTION ERROR';
    return `<article class="load-server-card ${escapeHtml(item.state || '')}">
      <header><div><p class="eyebrow">${nodeLabel}</p><h3>${escapeHtml(item.server)}</h3><span>${escapeHtml(item.url || '')}</span></div><span class="status-pill ${statusClass}">${stateLabel}</span></header>
      ${item.online ? `${loadBar('CPU', item.cpu_percent, thresholds.cpu || 85, cpuDetail)}${loadBar('RAM', item.memory_percent, thresholds.memory || 85, memoryDetail)}${loadBar('Диск', item.disk_percent, thresholds.disk || 90, diskDetail)}${extra}${limited}<footer><span>${escapeHtml(source)}</span><span>${item.latency_ms ?? '—'} ms</span></footer>` : `<div class="load-offline-message">${escapeHtml(item.error || 'Сервер недоступен')}</div>`}
    </article>`;
  }).join('');
  $('#load-server-grid').innerHTML = cards || '<div class="empty-state server-empty"><div class="empty-icon">◒</div><h3>Нет данных нагрузки</h3><p>Добавьте и включите Flussonic-серверы.</p></div>';

  const networkRows = [];
  for (const item of payload.servers || []) {
    for (const iface of item.network_interfaces || []) {
      networkRows.push(`<tr><td><strong>${escapeHtml(item.server)}</strong></td><td class="mono">${escapeHtml(iface.device)}${iface.virtual ? ' <span class="soft-badge">virtual</span>' : ''}</td><td><span class="status-pill ${iface.up ? 'alive' : 'danger'}">${iface.up ? 'UP' : 'DOWN'}</span></td><td>${formatBitrate(iface.rx_bps)}</td><td>${formatBitrate(iface.tx_bps)}</td><td>${formatBytes(iface.rx_total_bytes)}</td><td>${formatBytes(iface.tx_total_bytes)}</td></tr>`);
    }
  }
  $('#load-network-count').textContent = networkRows.length;
  $('#load-network-body').innerHTML = networkRows.join('') || monitorEmptyRow(7);

  if (state.loadRange === 'live') $('#load-chart-info').textContent = `${loadMetricLabel(state.loadMetric)} · ${state.loadHistory.length} точек`;
  requestAnimationFrame(drawLoadChart);
}

async function loadLoadHistory(range) {
  try {
    const serverParam = state.loadServerId !== 'all' ? `&server_id=${encodeURIComponent(state.loadServerId)}` : '';
    const data = await api(`/api/load/history?range=${range}${serverParam}`);
    state.loadHistory = (data.items || []).map(row => ({ ts: row.ts, ...(row.details || row) }));
    $('#load-chart-info').textContent = `История ${range}: ${state.loadHistory.length} точек`;
    drawLoadChart();
  } catch (error) { toast(error.message, 'error'); }
}

function drawLoadChart() {
  const canvas = $('#load-chart');
  const legendNode = $('#load-legend');
  if (!canvas || canvas.offsetParent === null) return;
  const rect = canvas.getBoundingClientRect();
  if (rect.width < 40 || rect.height < 40) return;
  const dpr = Math.max(1, window.devicePixelRatio || 1);
  canvas.width = Math.max(1, Math.round(rect.width * dpr));
  canvas.height = Math.max(1, Math.round(rect.height * dpr));
  const ctx = canvas.getContext('2d');
  ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
  const width = rect.width; const height = rect.height;
  ctx.clearRect(0, 0, width, height);

  const pad = { left: 54, right: 20, top: 20, bottom: 30 };
  const chartWidth = Math.max(1, width - pad.left - pad.right);
  const chartHeight = Math.max(1, height - pad.top - pad.bottom);
  ctx.fillStyle = 'rgba(8,12,24,.22)';
  ctx.fillRect(pad.left, pad.top, chartWidth, chartHeight);

  let points = state.loadHistory.filter(point => state.loadServerId === 'all' || point.server_id === state.loadServerId);
  points = points.filter(point => Number.isFinite(loadMetricValue(point, state.loadMetric)));
  const serverNames = new Map((state.loadPayload?.servers || []).map(item => [item.server_id, item.server]));

  if (!points.length) {
    ctx.fillStyle = '#7f8ba3'; ctx.font = '12px system-ui'; ctx.textAlign = 'center';
    ctx.fillText('Ожидание данных Node Exporter…', width / 2, height / 2);
    ctx.textAlign = 'left';
    if (legendNode) legendNode.innerHTML = '';
    return;
  }

  const groups = new Map();
  for (const point of points) {
    if (!groups.has(point.server_id)) groups.set(point.server_id, []);
    groups.get(point.server_id).push(point);
  }
  for (const series of groups.values()) series.sort((a, b) => a.ts - b.ts);

  let minTs = Math.min(...points.map(point => Number(point.ts)));
  let maxTs = Math.max(...points.map(point => Number(point.ts)));
  if (minTs === maxTs) { minTs -= 1; maxTs += 1; }
  const values = points.map(point => loadMetricValue(point, state.loadMetric)).filter(Number.isFinite);
  let maxValue = Math.max(1, ...values);
  if (['cpu_percent', 'memory_percent', 'disk_percent'].includes(state.loadMetric)) maxValue = 100;
  else maxValue *= 1.12;
  const x = ts => pad.left + chartWidth * ((ts - minTs) / Math.max(1, maxTs - minTs));
  const y = value => pad.top + chartHeight * (1 - Math.max(0, value) / maxValue);

  ctx.strokeStyle = 'rgba(153,166,194,.15)'; ctx.fillStyle = '#74819a'; ctx.font = '10px system-ui'; ctx.lineWidth = 1;
  ctx.textAlign = 'left';
  for (let index = 0; index <= 4; index += 1) {
    const value = maxValue * (1 - index / 4); const lineY = pad.top + chartHeight * (index / 4);
    ctx.beginPath(); ctx.moveTo(pad.left, lineY); ctx.lineTo(width - pad.right, lineY); ctx.stroke();
    const label = maxValue <= 100 ? value.toFixed(0) : value >= 1000 ? value.toFixed(0) : value.toFixed(1);
    ctx.fillText(label, 7, lineY + 3);
  }

  const palette = ['#23d5e6', '#7c5cff', '#2dd4a7', '#f8b84e', '#ff6577', '#72a7ff', '#c880ff', '#8ad46f'];
  let colorIndex = 0; const legend = [];
  for (const [serverId, series] of groups) {
    const color = palette[colorIndex % palette.length];
    ctx.beginPath();
    series.forEach((point, pointIndex) => {
      const px = x(Number(point.ts)); const py = y(loadMetricValue(point, state.loadMetric));
      if (pointIndex === 0) ctx.moveTo(px, py); else ctx.lineTo(px, py);
    });
    ctx.strokeStyle = color; ctx.lineWidth = 2.2; ctx.lineJoin = 'round'; ctx.lineCap = 'round'; ctx.stroke();
    const lastPoint = series.at(-1);
    const lastValue = loadMetricValue(lastPoint, state.loadMetric);
    const lastX = x(Number(lastPoint.ts)); const lastY = y(lastValue);
    ctx.beginPath(); ctx.arc(lastX, lastY, 3.5, 0, Math.PI * 2); ctx.fillStyle = color; ctx.fill();
    legend.push(`<span><i style="background:${color}"></i>${escapeHtml(serverNames.get(serverId) || lastPoint.server || serverId)} · ${Number(lastValue).toFixed(1)}</span>`);
    colorIndex += 1;
  }

  ctx.fillStyle = '#74819a'; ctx.font = '10px system-ui';
  const timeOptions = state.loadRange === 'live'
    ? { hour: '2-digit', minute: '2-digit', second: '2-digit' }
    : { hour: '2-digit', minute: '2-digit' };
  const first = new Date(minTs * 1000).toLocaleTimeString('ru-RU', timeOptions);
  const last = new Date(maxTs * 1000).toLocaleTimeString('ru-RU', timeOptions);
  ctx.fillText(first, pad.left, height - 7);
  ctx.fillText(last, width - pad.right - ctx.measureText(last).width, height - 7);
  if (legendNode) legendNode.innerHTML = legend.join('');
}

function monitorEmptyRow(columns) {
  return `<tr class="monitor-empty-row"><td colspan="${columns}">Нет данных для выбранного фильтра</td></tr>`;
}

function sortMonitorSessions(rows) {
  const keys = state.monitorSort === 'login'
    ? ['login', 'server', 'channel']
    : state.monitorSort === 'channel'
      ? ['channel', 'server', 'login']
      : ['server', 'login', 'channel'];
  return rows.slice().sort((left, right) => {
    for (const key of keys) {
      const result = String(left[key] ?? '').localeCompare(String(right[key] ?? ''), 'ru');
      if (result) return result;
    }
    return 0;
  });
}

function switchView(view) {
  state.currentView = view;
  $$('.nav-item').forEach(item => item.classList.toggle('active', item.dataset.view === view));
  $('#streams-view').classList.toggle('hidden', view !== 'streams');
  $('#servers-view').classList.toggle('hidden', view !== 'servers');
  ['monitor','load','cluster','placement','sync','sources','backups','audit','alerts'].forEach(name => $(`#${name}-view`)?.classList.toggle('hidden', view !== name));
  const titles = { streams: 'Потоки', servers: 'Серверы', monitor: 'Мониторинг', load: 'Нагрузка серверов', cluster: 'Cluster', placement: 'Размещение каналов', sync: 'Синхронизация', sources: 'Источники', backups: 'Резервные копии', audit: 'История действий', alerts: 'Уведомления' };
  $('#page-title').textContent = titles[view] || 'Панель';
  $('#add-stream-btn').classList.toggle('hidden', view !== 'streams');
  $('#import-m3u-btn').classList.toggle('hidden', view !== 'streams');
  if (view !== 'cluster') stopClusterMonitor();
  if (view === 'monitor') startMonitor();
  if (view === 'load') startLoadMonitor();
  if (view === 'cluster') startClusterMonitor();
  if (view === 'placement') loadPlacementOverview();
  if (view === 'sync') loadSyncOverview();
  if (view === 'sources') loadSourceChecks();
  if (view === 'backups') loadBackups();
  if (view === 'audit') loadAudit();
  if (view === 'alerts') loadAlerts();
  $('.sidebar').classList.remove('open');
}

function openServerModal(serverId = null) {
  state.currentServer = serverId ? state.servers.find(server => server.id === serverId) : null;
  const editing = Boolean(state.currentServer);
  $('#server-modal-title').textContent = editing ? 'Редактирование сервера' : 'Новый сервер';
  $('#server-submit').textContent = editing ? 'Сохранить сервер' : 'Добавить сервер';
  $('#server-id').disabled = editing;
  $('#server-id').value = state.currentServer?.id || '';
  $('#server-name').value = state.currentServer?.name || '';
  $('#server-url').value = state.currentServer?.url || '';
  $('#server-node-exporter-url').value = state.currentServer?.node_exporter_url || '';
  $('#server-username').value = state.currentServer?.username || '';
  $('#server-password').value = '';
  $('#server-password').required = !editing;
  $('#server-password').placeholder = editing ? 'Оставьте пустым, чтобы не менять' : 'Введите пароль';
  $('#server-password-help').textContent = editing ? 'Оставьте пустым, чтобы сохранить текущий пароль.' : 'Пароль сохранится зашифрованным.';
  $('#server-enabled').checked = editing ? state.currentServer.enabled : true;
  $('#server-primary').checked = editing ? state.currentServer.primary : state.servers.filter(server => server.enabled).length === 0;
  $('#server-verify-tls').checked = editing ? state.currentServer.verify_tls : true;
  $('#server-test-result').className = 'connection-result full hidden';
  $('#server-test-result').textContent = '';
  $('#server-dialog').showModal();
}

function serverFormPayload(includeEmptyPassword = false) {
  const payload = {
    name: $('#server-name').value.trim(),
    url: $('#server-url').value.trim().replace(/\/+$/, ''),
    username: $('#server-username').value.trim(),
    primary: $('#server-primary').checked,
    enabled: $('#server-enabled').checked,
    verify_tls: $('#server-verify-tls').checked,
    node_exporter_url: $('#server-node-exporter-url').value.trim().replace(/\/+$/, ''),
  };
  const id = $('#server-id').value.trim();
  const password = $('#server-password').value;
  if (id) payload.id = id;
  if (password || includeEmptyPassword) payload.password = password;
  return payload;
}

async function saveServer(event) {
  event.preventDefault();
  const button = $('#server-submit');
  const payload = serverFormPayload(!state.currentServer);
  if (!payload.name || !payload.url || !payload.username || (!state.currentServer && !payload.password)) {
    return toast('Заполните название, URL, логин и пароль', 'error');
  }
  setBusy(button, true);
  try {
    if (state.currentServer) {
      delete payload.id;
      await api(`/api/servers/${encodeURIComponent(state.currentServer.id)}`, { method: 'PUT', body: JSON.stringify(payload) });
      toast('Сервер сохранён');
    } else {
      await api('/api/servers', { method: 'POST', body: JSON.stringify(payload) });
      toast('Сервер добавлен');
    }
    $('#server-dialog').close();
    await loadServers();
    await loadStreams();
  } catch (error) {
    toast(error.message, 'error', 6500);
  } finally {
    setBusy(button, false);
  }
}

async function testServerForm() {
  const button = $('#test-server-btn');
  const resultBox = $('#server-test-result');
  const payload = serverFormPayload();
  if (!payload.url || !payload.username) return toast('Введите URL и API-логин', 'error');
  if (!payload.password && !state.currentServer) return toast('Введите API-пароль', 'error');
  if (!payload.password && state.currentServer && (payload.url !== state.currentServer.url || payload.username !== state.currentServer.username || payload.verify_tls !== state.currentServer.verify_tls)) {
    return toast('Введите пароль, чтобы проверить изменённые реквизиты', 'error');
  }
  setBusy(button, true, 'Проверка…');
  resultBox.className = 'connection-result full hidden';
  try {
    const data = payload.password
      ? await api('/api/servers/test', { method: 'POST', body: JSON.stringify({ url: payload.url, username: payload.username, password: payload.password, verify_tls: payload.verify_tls, node_exporter_url: payload.node_exporter_url || null }) })
      : await api(`/api/servers/${encodeURIComponent(state.currentServer.id)}/test`, { method: 'POST' });
    const exporter = data.node_exporter || {};
    const exporterText = exporter.ok ? `Node Exporter: OK · ${exporter.latency_ms} ms` : `Node Exporter: ${exporter.error || 'недоступен'}`;
    resultBox.className = `connection-result full ${data.ok ? 'ok' : 'error'}`;
    resultBox.textContent = data.ok ? `Flussonic: OK · ${data.latency_ms} ms · ${exporterText}` : `Flussonic: ошибка ${data.error || 'сервер недоступен'} · ${exporterText}`;
  } catch (error) {
    resultBox.className = 'connection-result full error';
    resultBox.textContent = `Ошибка: ${error.message}`;
  } finally {
    setBusy(button, false);
  }
}

async function testSavedServer(serverId, button) {
  setBusy(button, true, 'Проверка…');
  try {
    const result = await api(`/api/servers/${encodeURIComponent(serverId)}/test`, { method: 'POST' });
    const exporter = result.node_exporter || {};
    const exporterText = exporter.ok ? `Node Exporter ${exporter.latency_ms} ms` : `Node Exporter недоступен`;
    toast(result.ok ? `Flussonic ${result.latency_ms} ms · ${exporterText}` : `Flussonic недоступен: ${result.error} · ${exporterText}`, result.ok ? 'success' : 'error', 7000);
    await loadServers();
  } catch (error) { toast(error.message, 'error', 6000); }
  finally { setBusy(button, false); }
}

async function setPrimaryServer(serverId) {
  try {
    await api(`/api/servers/${encodeURIComponent(serverId)}/primary`, { method: 'POST' });
    toast('Основной сервер изменён');
    await loadServers();
  } catch (error) { toast(error.message, 'error'); }
}

async function deleteServer(serverId) {
  const server = state.servers.find(item => item.id === serverId);
  if (!server || !window.confirm(`Удалить сервер «${server.name}» из панели? Потоки на самом Flussonic удалены не будут.`)) return;
  try {
    await api(`/api/servers/${encodeURIComponent(serverId)}`, { method: 'DELETE' });
    toast('Сервер удалён из панели');
    await loadServers();
    await loadStreams();
  } catch (error) { toast(error.message, 'error'); }
}


function formatDate(ts) { return ts ? new Date(ts * 1000).toLocaleString('ru-RU') : '—'; }
function emptyRow(columns) { return `<tr class="monitor-empty-row"><td colspan="${columns}">Нет данных</td></tr>`; }

function updateBulkState() {
  const count = state.selectedStreams.size;
  $('#bulk-open-btn').disabled = count === 0;
  $('#bulk-ondemand-btn').disabled = count === 0;
  $('#bulk-static-btn').disabled = count === 0;
  $('#bulk-disable-btn').disabled = count === 0;
  $('#bulk-enable-btn').disabled = count === 0;
  $('#bulk-open-btn').textContent = count ? `Массовые операции · ${count}` : 'Массовые операции';
  const all = filteredStreams();
  $('#select-all-streams').checked = all.length > 0 && all.every(item => state.selectedStreams.has(item.name));
}

async function setStreamsMode(names, makeStatic, button = null) {
  if (!names.length) return;
  const label = makeStatic ? 'Static' : 'On demand';
  if (names.length > 1 && !window.confirm(`Перевести выбранные ${names.length} каналов в режим ${label}?`)) return;
  setBusy(button, true, '…');
  try {
    const result = await api('/api/stream-mode', { method: 'PUT', body: JSON.stringify({ names, static: makeStatic }) });
    const message = result.ok
      ? `${label}: ${result.stream_count} каналов · изменено на ${result.changed_count} CDN`
      : `${label}: ${result.stream_count} каналов · изменений ${result.changed_count}, ошибок ${result.failure_count}`;
    toast(message, result.ok ? 'success' : 'error', 6500);
    await loadStreams();
  } catch (error) {
    toast(error.message, 'error', 7000);
  } finally {
    setBusy(button, false);
    updateBulkState();
  }
}

async function setStreamsDisabled(names, disabled, button = null) {
  if (!names.length) return;
  const action = disabled ? 'временно отключить' : 'включить';
  const actionLabel = disabled ? 'Отключено' : 'Включено';
  const question = names.length > 1
    ? `${disabled ? 'Временно отключить' : 'Включить'} выбранные ${names.length} каналов?`
    : `${disabled ? 'Временно отключить' : 'Включить'} поток «${names[0]}»?`;
  if (!window.confirm(question)) return;
  setBusy(button, true, '…');
  try {
    const result = await api('/api/stream-state', { method: 'PUT', body: JSON.stringify({ names, disabled }) });
    const message = result.ok
      ? `${actionLabel}: ${result.stream_count} каналов · изменено на ${result.changed_count} CDN`
      : `${actionLabel}: ${result.stream_count} каналов · изменений ${result.changed_count}, ошибок ${result.failure_count}`;
    toast(message, result.ok ? 'success' : 'error', 6500);
    await loadStreams();
  } catch (error) {
    toast(`Не удалось ${action} поток: ${error.message}`, 'error', 7000);
  } finally {
    setBusy(button, false);
    updateBulkState();
  }
}

function openBulk() {
  if (!state.selectedStreams.size) return;
  $('#bulk-count-label').textContent = `Выбрано потоков: ${state.selectedStreams.size}`;
  $('#bulk-operation').value = 'add_input';
  $('#bulk-value').value = '';
  renderTargets($('#bulk-targets'));
  updateBulkValueField();
  $('#bulk-dialog').showModal();
}

function updateBulkValueField() {
  const op = $('#bulk-operation').value;
  const field = $('#bulk-value-field');
  field.classList.toggle('hidden', ['sync', 'delete'].includes(op));
  const input = $('#bulk-value');
  if (op === 'set_static') { input.placeholder = 'true или false'; input.value = 'false'; }
  else if (op === 'add_input') input.placeholder = 'hls:// или http://...';
  else input.placeholder = 'Новое значение';
}

async function saveBulk(event) {
  event.preventDefault();
  const button = $('#bulk-form button[type="submit"]');
  const operation = $('#bulk-operation').value;
  const targets = checkedTargets($('#bulk-targets'));
  if (!targets.length) return toast('Выберите серверы', 'error');
  let value = $('#bulk-value').value.trim();
  if (operation === 'set_static') value = ['true','1','yes','on'].includes(value.toLowerCase());
  if (!['sync','delete'].includes(operation) && value === '') return toast('Введите значение', 'error');
  if (operation === 'delete' && !confirm(`Удалить ${state.selectedStreams.size} потоков на выбранных серверах? Резервные копии будут сохранены.`)) return;
  setBusy(button, true, 'Выполнение…');
  try {
    const result = await api('/api/bulk', { method: 'POST', body: JSON.stringify({ names: [...state.selectedStreams], operation, value, target_ids: targets, source_id: state.primaryId }) });
    toast(result.ok ? `Готово: ${result.success_count}` : `Частично: ${result.success_count}, ошибок ${result.failure_count}`, result.ok ? 'success' : 'error', 7000);
    $('#bulk-dialog').close(); state.selectedStreams.clear(); await loadStreams();
  } catch (error) { toast(error.message, 'error', 7000); }
  finally { setBusy(button, false); updateBulkState(); }
}

async function openPreview(name) {
  $('#preview-title').textContent = name;
  $('#preview-frame').src = 'about:blank';
  $('#preview-dialog').showModal();
  try {
    const data = await api(`/api/preview/${encodeURIComponent(name)}?server_id=${encodeURIComponent(state.selectedServerId)}`);
    $('#preview-frame').src = data.embed_url;
    $('#preview-hls').href = data.hls_url; $('#preview-llhls').href = data.ll_hls_url;
  } catch (error) { toast(error.message, 'error'); }
}

async function openDiagnostics(name) {
  $('#diagnostics-title').textContent = name; $('#diagnostics-summary').innerHTML = '<div class="loading-card">Загрузка…</div>';
  $('#diagnostics-dialog').showModal();
  try {
    const d = await api(`/api/diagnostics/${encodeURIComponent(name)}?server_id=${encodeURIComponent(state.selectedServerId)}`);
    const s = d.status || {};
    $('#diagnostics-summary').innerHTML = [
      ['Статус', s.status || 'unknown'], ['Alive', String(Boolean(s.alive))], ['Running', String(Boolean(s.running))],
      ['Активный input', s.active_input || 'не определён'], ['Input bitrate', `${s.inputs_bandwidth || 0} bps`],
      ['Output bitrate', `${s.output_bandwidth || 0} bps`], ['Сессии', s.playback_sessions ?? 0], ['Coder error', String(Boolean(s.coder_error))]
    ].map(([k,v]) => `<article class="diagnostic-card"><span>${escapeHtml(k)}</span><strong>${escapeHtml(v)}</strong></article>`).join('');
    $('#diagnostics-media').textContent = JSON.stringify(d.media_info || {}, null, 2);
    $('#diagnostics-raw').textContent = JSON.stringify(d.raw_stats || {}, null, 2);
  } catch (error) { $('#diagnostics-summary').innerHTML = `<p class="form-error">${escapeHtml(error.message)}</p>`; }
}

async function loadPlacementSettings() {
  try {
    const data = await api('/api/placement/settings');
    state.placementEnabled = Boolean(data.enabled);
    if ($('#placement-enabled')) $('#placement-enabled').checked = state.placementEnabled;
    updatePlacementModeCopy();
  } catch (_) {}
}

function updatePlacementModeCopy() {
  const enabled = state.placementEnabled;
  if ($('#placement-mode-title')) $('#placement-mode-title').textContent = enabled ? 'Гибридная' : 'Зеркальная';
  if ($('#placement-mode-help')) $('#placement-mode-help').textContent = enabled
    ? 'Назначенные каналы работают на одном CDN; остальные продолжают зеркалироваться.'
    : 'Все каналы должны быть на всех включённых серверах. Сохранённые назначения временно не применяются.';
}

async function togglePlacementMode() {
  const enabled = $('#placement-enabled').checked;
  try {
    await api('/api/placement/settings', { method: 'PUT', body: JSON.stringify({ enabled }) });
    state.placementEnabled = enabled; updatePlacementModeCopy();
    toast(enabled ? 'Гибридное размещение включено' : 'Зеркальный режим включён');
    await Promise.all([loadPlacementOverview(), loadStreams()]);
  } catch (error) { $('#placement-enabled').checked = state.placementEnabled; toast(error.message, 'error'); }
}

async function loadPlacementOverview() {
  $('#placement-body').innerHTML = '<tr class="loading-row"><td colspan="6">Сканирование размещения…</td></tr>';
  try {
    const data = await api('/api/placement/overview');
    state.placementEnabled = Boolean(data.enabled); state.placementItems = data.items || []; state.placementServerCounts = data.server_counts || []; state.placementSelected.clear();
    $('#placement-enabled').checked = state.placementEnabled; updatePlacementModeCopy();
    fillPlacementServerSelect($('#placement-server-select'));
    $('#placement-total').textContent = data.summary?.total ?? 0;
    $('#placement-assigned').textContent = data.summary?.assigned ?? 0;
    $('#placement-mirror').textContent = data.summary?.mirror ?? 0;
    $('#placement-drift').textContent = data.summary?.drift ?? 0;
    renderPlacementOverview();
  } catch (error) { $('#placement-body').innerHTML = `<tr><td colspan="6">${escapeHtml(error.message)}</td></tr>`; }
}

function placementActualText(item) {
  const present = item.states.filter(state => state.present).map(state => state.server_name);
  return present.length ? present.join(', ') : 'Нигде';
}

function renderPlacementOverview() {
  const q = ($('#placement-filter').value || '').toLowerCase(); const filter = $('#placement-status-filter').value;
  const items = state.placementItems.filter(item => {
    const mode = item.placement?.configured_mode || 'mirror';
    return (!q || item.name.toLowerCase().includes(q)) && (filter === 'all' || filter === mode || (filter === 'drift' && !item.in_sync));
  });
  $('#placement-count').textContent = `${items.length}`;
  $('#placement-body').innerHTML = items.map(item => {
    const assigned = item.placement?.configured_mode === 'assigned';
    const inactive = assigned && !state.placementEnabled;
    return `<tr data-name="${escapeHtml(item.name)}"><td><input class="placement-row-check" type="checkbox" ${state.placementSelected.has(item.name) ? 'checked' : ''}></td><td><strong>${escapeHtml(item.name)}</strong></td><td><span class="status-pill ${assigned ? 'waiting' : 'alive'}">${assigned ? 'Один CDN' : 'Зеркало'}</span>${inactive ? '<span class="cell-sub">назначение сохранено, но выключено</span>' : ''}</td><td>${assigned ? escapeHtml(item.placement.primary_server_name || 'сервер удалён') : 'Все включённые CDN'}</td><td class="url-cell">${escapeHtml(placementActualText(item))}</td><td><span class="status-pill ${item.in_sync ? 'alive' : 'waiting'}">${item.in_sync ? 'Готово' : 'Нужно применить'}</span></td></tr>`;
  }).join('') || emptyRow(6);
  $$('.placement-row-check').forEach(box => box.addEventListener('change', event => { const name=event.target.closest('tr').dataset.name; event.target.checked ? state.placementSelected.add(name) : state.placementSelected.delete(name); updatePlacementSelection(); }));
  updatePlacementSelection();
}

function updatePlacementSelection() {
  const count = state.placementSelected.size;
  $('#placement-assign-btn').disabled = !count || !state.placementEnabled;
  $('#placement-mirror-btn').disabled = !count;
  $('#placement-apply-btn').disabled = !count;
  const visible = $$('.placement-row-check');
  $('#placement-select-all').checked = visible.length > 0 && visible.every(box => box.checked);
}

async function assignPlacement(mode) {
  const names = [...state.placementSelected]; if (!names.length) return;
  const serverId = mode === 'assigned' ? $('#placement-server-select').value : null;
  if (mode === 'assigned' && !serverId) return toast('Выберите CDN', 'error');
  if (mode === 'assigned') {
    const capacity=(state.placementServerCounts || []).find(item => item.server_id === serverId);
    if (capacity && capacity.assigned + names.length > capacity.capacity && !confirm(`${capacity.server_name}: после назначения может быть больше ${capacity.capacity} каналов. Продолжить?`)) return;
  }
  try {
    await api('/api/placement/bulk/assign', { method: 'PUT', body: JSON.stringify({ names, mode, server_id: serverId }) });
    toast(mode === 'assigned' ? `Каналы назначены на ${state.servers.find(server => server.id === serverId)?.name || serverId}` : 'Каналы возвращены в зеркало');
    await loadPlacementOverview();
  } catch (error) { toast(error.message, 'error'); }
}

async function applyPlacement() {
  const names = [...state.placementSelected]; if (!names.length) return;
  const removeExtras = $('#placement-remove-extras').checked;
  const text = removeExtras ? `Применить размещение для ${names.length} каналов и удалить лишние копии? Перед удалением будут созданы резервные копии.` : `Создать и проверить назначенные копии для ${names.length} каналов? Лишние копии останутся работать.`;
  if (!confirm(text)) return;
  const button=$('#placement-apply-btn'); setBusy(button,true,'Применение…');
  try {
    const result=await api('/api/placement/apply',{method:'POST',body:JSON.stringify({names,remove_extras:removeExtras})});
    toast(result.ok ? `Готово: ${result.success_count} операций` : `Ошибок: ${result.failure_count}`, result.ok ? 'success' : 'error', 7000);
    await Promise.all([loadPlacementOverview(), loadSyncOverview()]);
  } catch(error){toast(error.message,'error',7000);} finally{setBusy(button,false);}
}

function updateStreamPlacementFields() {
  const assigned = $('#stream-placement-mode').value === 'assigned';
  $('#stream-placement-server-field').classList.toggle('hidden', !assigned);
  $('#stream-targets-field').classList.toggle('hidden', assigned);
  if (assigned) {
    const selected=$('#stream-placement-server').value;
    renderTargets($('#create-targets'), selected ? [selected] : []);
  } else renderTargets($('#create-targets'), state.servers.filter(server=>server.enabled&&server.online).map(server=>server.id));
}

async function loadSyncOverview() {
  $('#sync-body').innerHTML = '<tr class="loading-row"><td colspan="4">Сравнение серверов…</td></tr>';
  try { const data = await api('/api/sync/overview'); state.syncItems = data.items || []; state.placementEnabled = Boolean(data.placement_enabled); renderSyncOverview(); }
  catch (error) { $('#sync-body').innerHTML = `<tr><td colspan="4">${escapeHtml(error.message)}</td></tr>`; }
}

function syncStateLabel(item) {
  if (item.state === 'ok') return ['ok', 'OK'];
  if (item.state === 'missing') return ['bad', 'нет · нужен'];
  if (item.state === 'different') return ['warn', `другая${(item.differences || []).length ? `: ${item.differences.join(', ')}` : ''}`];
  if (item.state === 'extra') return ['warn', 'лишняя копия'];
  if (item.state === 'error') return ['bad', 'ошибка'];
  return ['', 'не требуется'];
}
function renderSyncOverview() {
  const q = ($('#sync-filter').value || '').toLowerCase(); const f = $('#sync-status-filter').value;
  const items = state.syncItems.filter(x => (!q || x.name.toLowerCase().includes(q)) && (f === 'all' || (f === 'drift' && !x.in_sync) || (f === 'missing' && x.states.some(s => s.state === 'missing'))));
  $('#sync-count').textContent = `${items.length}`;
  $('#sync-body').innerHTML = items.map(x => `<tr data-name="${escapeHtml(x.name)}"><td><input class="sync-row-check" type="checkbox"></td><td><strong>${escapeHtml(x.name)}</strong><span class="cell-sub">${x.placement?.effective_mode === 'assigned' ? `назначен: ${escapeHtml(x.placement.primary_server_name || 'не найден')}` : 'зеркальный'}</span></td><td><span class="status-pill ${x.in_sync ? 'alive' : 'waiting'}">${x.in_sync ? 'Синхронно' : 'Различия'}</span></td><td><div class="sync-server-pills">${x.states.map(s => { const [cls,label]=syncStateLabel(s); return `<span class="mini-state ${cls}">${escapeHtml(s.server_name)} · ${escapeHtml(label)}</span>`; }).join('')}</div></td></tr>`).join('') || emptyRow(4);
  $$('.sync-row-check').forEach(c => c.addEventListener('change', updateSyncSelection)); updateSyncSelection();
}
function updateSyncSelection() { const n = $$('.sync-row-check:checked').length; $('#sync-selected-btn').disabled = !n; $('#sync-select-all').checked = n > 0 && n === $$('.sync-row-check').length; }
async function syncSelected() {
  const names = $$('.sync-row-check:checked').map(c => c.closest('tr').dataset.name); if (!names.length) return;
  const button=$('#sync-selected-btn'); setBusy(button,true,'Синхронизация…');
  try {
    const result = state.placementEnabled
      ? await api('/api/placement/apply',{method:'POST',body:JSON.stringify({names,remove_extras:false})})
      : await api('/api/bulk',{method:'POST',body:JSON.stringify({names,operation:'sync',source_id:state.primaryId})});
    toast(result.ok?'Синхронизация завершена':`Ошибок: ${result.failure_count}`,result.ok?'success':'error'); await loadSyncOverview();
  } catch(e){toast(e.message,'error');} finally{setBusy(button,false);}
}

async function loadSourceChecks() {
  try {
    const data = await api('/api/source-checks');
    state.sourceItems = data.items || [];
    $('#sources-interval').textContent = data.background ? `${Math.round(Number(data.interval || 0) / 60)} мин` : 'Ручной';
    renderSourceChecks();
  } catch (e) { toast(e.message, 'error'); }
}

function sourceStateClass(value) {
  if (value === 'ok' || value === 'reachable') return 'alive';
  if (value === 'disabled' || value === 'unsupported') return 'waiting';
  return '';
}

function renderSourceChecks() {
  const q = ($('#sources-filter').value || '').trim().toLowerCase();
  const status = $('#sources-status-filter')?.value || 'all';
  const isHealthy = item => ['ok', 'reachable'].includes(item.state);
  const matchesStatus = item => {
    if (status === 'failed') return item.state === 'failed';
    if (status === 'healthy') return isHealthy(item);
    if (status === 'unsupported') return item.state === 'unsupported';
    return true;
  };
  const items = state.sourceItems.filter(x => {
    const matchesSearch = !q || [x.server_name, x.stream_name, x.input_url, x.detail].join(' ').toLowerCase().includes(q);
    return matchesSearch && matchesStatus(x);
  });
  const failedItems = state.sourceItems.filter(x => x.state === 'failed');
  const failedStreams = new Set(failedItems.map(x => `${x.server_id || x.server_name}::${x.stream_name}`));
  $('#sources-total').textContent = state.sourceItems.length;
  $('#sources-ok').textContent = state.sourceItems.filter(isHealthy).length;
  $('#sources-failed').textContent = failedItems.length;
  $('#sources-problem-streams').textContent = `Проблемных потоков: ${failedStreams.size}`;
  $('#sources-count').textContent = status === 'all' && !q ? `${items.length}` : `Показано: ${items.length}`;
  $('#sources-body').innerHTML = items.map((x, index) => {
    const detailClass = x.state === 'failed' ? 'danger-text' : '';
    return `<tr>
      <td><strong>${escapeHtml(x.stream_name)}</strong><span class="cell-sub">${escapeHtml(x.server_name)}</span></td>
      <td class="mono url-cell">${escapeHtml(x.input_url)}</td>
      <td><span class="status-pill ${sourceStateClass(x.state)}">${escapeHtml(x.state)}</span>${x.detail ? `<span class="cell-sub ${detailClass}">${escapeHtml(x.detail)}</span>` : ''}</td>
      <td>${x.latency_ms ?? '—'} ms</td>
      <td>${formatDate(x.checked_at)}</td>
      <td><div class="inline-actions"><button class="btn ghost small source-check-one" data-index="${index}">Проверить</button><button class="btn ${x.state === 'failed' ? 'danger' : 'ghost'} small source-action-open" data-index="${index}">Действия</button></div></td>
    </tr>`;
  }).join('') || emptyRow(6);
  $$('.source-check-one', $('#sources-body')).forEach(button => button.addEventListener('click', () => runSourceCheckFor(items[Number(button.dataset.index)], button)));
  $$('.source-action-open', $('#sources-body')).forEach(button => button.addEventListener('click', () => openSourceAction(items[Number(button.dataset.index)])));
}

async function runSourceChecks() {
  const b = $('#sources-run-btn');
  setBusy(b, true, 'Проверка…');
  try {
    const r = await api('/api/source-checks/run', { method: 'POST' });
    const removed = Number(r.removed_stale || 0);
    const cleanupText = removed ? `, удалено устаревших записей ${removed}` : '';
    toast(`Проверено ${r.checked}, ошибок ${r.failed}${cleanupText}`, r.failed ? 'error' : 'success');
    await loadSourceChecks();
  } catch (e) { toast(e.message, 'error'); } finally { setBusy(b, false); }
}

async function runSourceCheckFor(item, button) {
  if (!item) return;
  setBusy(button, true, 'Проверка…');
  try {
    const query = new URLSearchParams({ stream_name: item.stream_name, server_id: item.server_id });
    const r = await api(`/api/source-checks/run?${query.toString()}`, { method: 'POST' });
    toast(`Поток ${item.stream_name}: проверено ${r.checked}, ошибок ${r.failed}`, r.failed ? 'error' : 'success');
    await loadSourceChecks();
  } catch (e) { toast(e.message, 'error'); } finally { setBusy(button, false); }
}

function openSourceAction(item) {
  if (!item) return;
  state.sourceActionItem = item;
  $('#source-action-title').textContent = item.stream_name;
  $('#source-action-context').textContent = `${item.server_name} · ${item.input_url}`;
  $('#source-action-type').value = item.state === 'disabled' ? 'enable_stream' : (item.state === 'failed' ? 'add_input' : 'promote_input');
  $('#source-new-input').value = '';
  updateSourceActionFields();
  $('#source-action-dialog').showModal();
}

function updateSourceActionFields() {
  const action = $('#source-action-type').value;
  $('#source-new-input-field').classList.toggle('hidden', action !== 'add_input');
  const warnings = {
    add_input: 'Новый источник добавится в конец списка и станет резервным.',
    promote_input: 'Выбранный input переместится на первое место и станет основным.',
    remove_input: 'Input будет удалён. Последний источник удалить нельзя.',
    disable_stream: 'Поток станет неактивным, но конфигурация сохранится. Его можно включить обратно.',
    enable_stream: 'Поток будет снова активирован и сразу проверен.',
  };
  $('#source-action-warning').querySelector('p').textContent = `${warnings[action]} Перед изменением создаётся резервная копия.`;
  $('#source-action-submit').classList.toggle('danger', ['remove_input', 'disable_stream'].includes(action));
  $('#source-action-submit').classList.toggle('primary', !['remove_input', 'disable_stream'].includes(action));
}

async function saveSourceAction(event) {
  event.preventDefault();
  const item = state.sourceActionItem;
  if (!item) return;
  const action = $('#source-action-type').value;
  const newInput = $('#source-new-input').value.trim();
  if (action === 'add_input' && !newInput) return toast('Введите новый input URL', 'error');
  if (['remove_input', 'disable_stream'].includes(action)) {
    const text = action === 'disable_stream' ? `Временно отключить поток ${item.stream_name}?` : `Удалить этот input из ${item.stream_name}?`;
    if (!confirm(text)) return;
  }
  const button = $('#source-action-submit');
  setBusy(button, true, 'Выполнение…');
  try {
    const result = await api('/api/source-checks/action', {
      method: 'POST',
      body: JSON.stringify({ server_id: item.server_id, stream_name: item.stream_name, input_url: item.input_url, action, new_input: newInput || null }),
    });
    toast(result.action === 'disable_stream' ? 'Поток временно отключён' : 'Действие выполнено');
    $('#source-action-dialog').close();
    await Promise.all([loadSourceChecks(), loadBackups(), loadAudit()]);
    if (state.currentView === 'streams' || item.server_id === state.selectedServerId) await loadStreams();
  } catch (e) { toast(e.message, 'error'); } finally { setBusy(button, false); }
}


function formatClusterUptime(seconds) {
  if (seconds === null || seconds === undefined || !Number.isFinite(Number(seconds))) return '—';
  let value = Math.max(0, Math.floor(Number(seconds)));
  const days = Math.floor(value / 86400); value %= 86400;
  const hours = Math.floor(value / 3600); value %= 3600;
  const minutes = Math.floor(value / 60);
  return days ? `${days}д ${hours}ч` : hours ? `${hours}ч ${minutes}м` : `${minutes}м`;
}

function clusterLoadText(row) {
  if (row.load === null || row.load === undefined) return '—';
  if (row.load_unit === '%') return `${Number(row.load).toFixed(1)}%`;
  if (row.load_unit === 'kbps') return `${Number(row.load).toFixed(0)} kbps`;
  return `${Number(row.load).toFixed(0)} ${row.load_unit || ''}`.trim();
}

async function loadClusterOverview(refresh = false) {
  try {
    const payload = await api(`/api/cluster/overview${refresh ? '?refresh=true' : ''}`);
    state.clusterPayload = payload;
    renderClusterOverview();
  } catch (error) {
    $('#cluster-body').innerHTML = `<tr><td colspan="8">${escapeHtml(error.message)}</td></tr>`;
    $('#cluster-live-dot').classList.add('offline');
  }
}

function renderClusterOverview() {
  const payload = state.clusterPayload || {};
  const summary = payload.summary || {};
  const settings = payload.settings || {};
  const rows = payload.nodes || [];
  $('#cluster-online').textContent = `${summary.online || 0}/${summary.nodes || 0}`;
  $('#cluster-clients').textContent = summary.clients || 0;
  $('#cluster-streams').textContent = summary.streams || 0;
  $('#cluster-output').textContent = `${Number(summary.output_bitrate_kbps || 0).toLocaleString('ru-RU', { maximumFractionDigits: 0 })} kbps`;
  $('#cluster-average-load').textContent = summary.average_load === null || summary.average_load === undefined ? '—' : Number(summary.average_load).toFixed(1);
  $('#cluster-mode-foot').textContent = `режим ${settings.mode || 'clients'}`;
  $('#cluster-updated').textContent = `Обновлено ${new Date((payload.ts || Date.now()/1000) * 1000).toLocaleTimeString('ru-RU')}`;
  $('#cluster-live-dot').classList.toggle('offline', !settings.enabled || !(summary.online || 0));
  const balancer = payload.balancer || {};
  $('#cluster-balancer-name').textContent = balancer.server_name ? `${settings.balancer_name || 'lb01'} · ${balancer.server_name}` : (settings.balancer_name || 'Cluster не настроен');
  const status = $('#cluster-balancer-status');
  status.className = `status-pill ${balancer.online ? 'alive' : settings.enabled ? 'danger' : 'waiting'}`;
  status.textContent = balancer.online ? 'Online' : settings.enabled ? 'Offline' : 'Выключен';
  $('#cluster-body').innerHTML = rows.map(row => `<tr>
    <td><strong>${escapeHtml(row.host || row.server_name)}</strong><span class="cell-sub">${escapeHtml(row.server_name || '')}</span></td>
    <td>${loadPercent(row.cpu_percent)}</td><td>${loadPercent(row.memory_percent)}</td>
    <td>${row.clients ?? 0}</td><td>${row.streams ?? 0}</td>
    <td>${Number(row.output_bitrate_kbps || 0).toLocaleString('ru-RU', { maximumFractionDigits: 1 })}</td>
    <td><span class="status-pill ${row.online ? (row.state === 'warning' || row.state === 'critical' ? 'waiting' : 'alive') : 'danger'}">${clusterLoadText(row)}</span></td>
    <td>${formatClusterUptime(row.uptime_seconds)}</td></tr>`).join('') || emptyRow(8);
}

function startClusterMonitor() {
  loadClusterOverview();
  clearInterval(state.clusterTimer);
  state.clusterTimer = setInterval(() => { if (state.currentView === 'cluster') loadClusterOverview(false); }, 5000);
}

function stopClusterMonitor() {
  clearInterval(state.clusterTimer); state.clusterTimer = null;
}

async function openClusterSettings() {
  try {
    const data = await api('/api/cluster/settings');
    state.clusterSettings = data.settings || {};
    const settings = state.clusterSettings;
    $('#cluster-enabled').checked = settings.enabled !== false;
    $('#cluster-balancer-id').value = settings.balancer_name || 'lb01';
    $('#cluster-mode').value = settings.mode || 'clients';
    $('#cluster-key').value = '';
    $('#cluster-key-help').textContent = settings.has_cluster_key ? 'Ключ уже сохранён. Оставьте пустым, чтобы не менять.' : 'Укажите общий cluster_key.';
    $('#cluster-balancer-server').innerHTML = '<option value="">Не выбран</option>' + state.servers.map(server => `<option value="${escapeHtml(server.id)}">${escapeHtml(server.name)}</option>`).join('');
    $('#cluster-balancer-server').value = settings.balancer_server_id || '';
    const peers = new Map((settings.peers || []).map(peer => [peer.server_id, peer]));
    $('#cluster-peer-list').innerHTML = state.servers.filter(server => server.enabled).map(server => {
      const peer = peers.get(server.id);
      let host = peer?.host || '';
      if (!host) { try { host = new URL(server.url).hostname; } catch (_) { host = server.name; } }
      return `<div class="cluster-peer-row" data-server-id="${escapeHtml(server.id)}"><label><input class="cluster-peer-check" type="checkbox" ${peer ? 'checked' : ''}> <strong>${escapeHtml(server.name)}</strong></label><input class="cluster-peer-host" value="${escapeHtml(host)}" placeholder="cdn-1.example.com"><input class="cluster-peer-limit" value="${escapeHtml(peer?.max_bitrate || '')}" placeholder="max 40M"></div>`;
    }).join('') || '<p class="muted">Нет включённых серверов.</p>';
    $('#cluster-dialog').showModal();
  } catch (error) { toast(error.message, 'error'); }
}

async function saveClusterSettings(event) {
  event.preventDefault();
  const button = $('#cluster-submit');
  const peers = $$('.cluster-peer-row').filter(row => $('.cluster-peer-check', row).checked).map(row => ({
    server_id: row.dataset.serverId,
    host: $('.cluster-peer-host', row).value.trim(),
    max_bitrate: $('.cluster-peer-limit', row).value.trim() || null,
  }));
  if ($('#cluster-enabled').checked && !peers.length) return toast('Выберите хотя бы один peer', 'error');
  const payload = {
    enabled: $('#cluster-enabled').checked,
    balancer_server_id: $('#cluster-balancer-server').value || null,
    balancer_name: $('#cluster-balancer-id').value.trim() || 'lb01',
    mode: $('#cluster-mode').value,
    cluster_key: $('#cluster-key').value.trim() || null,
    peers,
  };
  setBusy(button, true);
  try {
    await api('/api/cluster/settings', { method: 'PUT', body: JSON.stringify(payload) });
    $('#cluster-dialog').close(); toast('Настройки Cluster сохранены'); await loadClusterOverview(true);
  } catch (error) { toast(error.message, 'error', 6500); } finally { setBusy(button, false); }
}

async function toggleClusterConfig() {
  const preview = $('#cluster-config-preview');
  if (!preview.classList.contains('hidden')) { preview.classList.add('hidden'); $('#cluster-config-btn').textContent = 'Показать конфиг'; return; }
  try {
    const data = await api('/api/cluster/config');
    preview.textContent = data.config || '';
    preview.classList.remove('hidden'); $('#cluster-config-btn').textContent = 'Скрыть конфиг';
  } catch (error) { toast(error.message, 'error'); }
}

async function loadBackups(){try{const d=await api('/api/backups?limit=500');state.backupItems=d.items||[];renderBackups();}catch(e){toast(e.message,'error');}}
function renderBackups(){const q=($('#backups-filter').value||'').toLowerCase();const items=state.backupItems.filter(x=>!q||[x.stream_name,x.server_name,x.action].join(' ').toLowerCase().includes(q));$('#backups-count').textContent=items.length;$('#backups-body').innerHTML=items.map(x=>`<tr><td>${formatDate(x.ts)}</td><td><strong>${escapeHtml(x.stream_name)}</strong></td><td>${escapeHtml(x.server_name)}</td><td>${escapeHtml(x.action)}</td><td class="mono">${escapeHtml(x.config_hash||'—')}</td><td><button class="btn ghost small restore-backup" data-id="${x.id}">Откатить</button></td></tr>`).join('')||emptyRow(6);$$('.restore-backup').forEach(b=>b.addEventListener('click',()=>restoreBackup(b.dataset.id)));}
async function restoreBackup(id){const item=state.backupItems.find(x=>String(x.id)===String(id));if(!confirm(`Восстановить ${item?.stream_name||'поток'} из версии #${id}? Текущая конфигурация тоже будет сохранена.`))return;try{await api(`/api/backups/${id}/restore`,{method:'POST'});toast('Конфигурация восстановлена');await Promise.all([loadBackups(),loadStreams(),loadAudit()]);}catch(e){toast(e.message,'error');}}

async function loadAudit(){try{const d=await api('/api/audit?limit=700');state.auditItems=d.items||[];renderAudit();}catch(e){toast(e.message,'error');}}
function renderAudit(){const q=($('#audit-filter').value||'').toLowerCase();const items=state.auditItems.filter(x=>!q||[x.actor,x.action,x.entity_id,x.summary].join(' ').toLowerCase().includes(q));$('#audit-count').textContent=items.length;$('#audit-body').innerHTML=items.map(x=>`<tr><td>${formatDate(x.ts)}</td><td>${escapeHtml(x.actor)}</td><td class="mono">${escapeHtml(x.action)}</td><td>${escapeHtml(x.entity_id||'—')}</td><td><span class="status-pill ${x.status==='success'?'alive':x.status==='partial'?'waiting':''}">${escapeHtml(x.status)}</span></td><td>${escapeHtml(x.summary)}</td></tr>`).join('')||emptyRow(6);}

async function loadAlerts(){try{const [s,l]=await Promise.all([api('/api/notifications/settings'),api('/api/notifications')]);$('#alerts-enabled').checked=s.enabled;$('#alerts-chat-id').value=s.telegram_chat_id||'';$$('#alerts-form .event-checks input').forEach(c=>c.checked=(s.events||[]).includes(c.value));state.alertItems=l.items||[];renderAlerts();}catch(e){toast(e.message,'error');}}
function renderAlerts(){ $('#alerts-count').textContent=state.alertItems.length;$('#alerts-body').innerHTML=state.alertItems.map(x=>`<tr><td>${formatDate(x.ts)}</td><td>${escapeHtml(x.event_type)}</td><td>${escapeHtml(x.message)}</td><td><span class="status-pill ${x.delivered?'alive':''}">${x.delivered?'Доставлено':'Ошибка'}</span>${x.error?`<span class="cell-sub danger-text">${escapeHtml(x.error)}</span>`:''}</td></tr>`).join('')||emptyRow(4); }
async function saveAlerts(e){e.preventDefault();const b=$('#alerts-form button[type="submit"]');setBusy(b,true);try{await api('/api/notifications/settings',{method:'PUT',body:JSON.stringify({enabled:$('#alerts-enabled').checked,telegram_chat_id:$('#alerts-chat-id').value.trim(),telegram_bot_token:$('#alerts-token').value.trim()||null,webhook_url:$('#alerts-webhook').value.trim()||null,events:$$('#alerts-form .event-checks input:checked').map(x=>x.value)})});$('#alerts-token').value='';$('#alerts-webhook').value='';toast('Настройки сохранены');}catch(e){toast(e.message,'error');}finally{setBusy(b,false);}}
async function testAlert(){try{const r=await api('/api/notifications/test',{method:'POST',body:JSON.stringify({message:'✅ Тест Cyrius Stream Control v5'})});toast(r.ok?'Тест доставлен':'Канал доставки не настроен',r.ok?'success':'error');await loadAlerts();}catch(e){toast(e.message,'error');}}

async function loadMonitorHistory(range){try{const d=await api(`/api/monitor/history?range=${range}`);state.monitorHistory=(d.items||[]).map(x=>({ts:x.ts,total:x.sessions,counts:Object.fromEntries((x.server_counts||[]).map(s=>[s.server_id,s.sessions]))}));drawMonitorChart();$('#monitor-chart-info').textContent=`История ${range}: ${state.monitorHistory.length} точек`;}catch(e){toast(e.message,'error');}}
$('#login-form').addEventListener('submit', async event => {
  event.preventDefault();
  $('#login-error').textContent = '';
  try {
    const data = await api('/api/auth/login', {
      method: 'POST', body: JSON.stringify({ username: $('#login-user').value, password: $('#login-password').value }),
    });
    state.user = data.username;
    $('#profile-user').textContent = data.username;
    showApp();
    await loadServers();
    await loadStreams();
  } catch (error) { $('#login-error').textContent = error.message; }
});

$('#logout-btn').addEventListener('click', async () => { try { await api('/api/auth/logout', { method: 'POST' }); } finally { stopMonitor(); stopLoadMonitor(); stopClusterMonitor(); showLogin(); } });
$('#refresh-btn').addEventListener('click', async () => {
  if (state.currentView === 'monitor') await loadMonitorSnapshot(true);
  else if (state.currentView === 'load') await loadLoadSnapshot(true);
  else if (state.currentView === 'cluster') await loadClusterOverview(true);
  else if (state.currentView === 'placement') await loadPlacementOverview();
  else if (state.currentView === 'sync') await loadSyncOverview();
  else if (state.currentView === 'sources') await loadSourceChecks();
  else if (state.currentView === 'backups') await loadBackups();
  else if (state.currentView === 'audit') await loadAudit();
  else if (state.currentView === 'alerts') await loadAlerts();
  else { await loadServers(); if (state.currentView === 'streams') await loadStreams(); }
  toast('Данные обновлены');
});
$('#add-stream-btn').addEventListener('click', () => openStreamModal());
$('#import-m3u-btn').addEventListener('click', openM3UImport);
$('#m3u-preview-btn').addEventListener('click', previewM3UImport);
$('#m3u-import-form').addEventListener('submit', applyM3UImport);
$('#m3u-placement-mode').addEventListener('change', updateM3UPlacementFields);
$('#add-server-btn').addEventListener('click', () => openServerModal());
$('#empty-add-server-btn').addEventListener('click', () => openServerModal());
$('#server-select').addEventListener('change', event => { state.selectedServerId = event.target.value; loadStreams(); });
$('#search-input').addEventListener('input', renderStreams);
$('#status-filter').addEventListener('change', renderStreams);
$('#monitor-filter').addEventListener('input', () => { if (state.monitorPayload) renderMonitor(state.monitorPayload, false); });
$('#load-server-select').addEventListener('change', event => { state.loadServerId = event.target.value; if (state.loadRange === 'live') drawLoadChart(); else loadLoadHistory(state.loadRange); });
$('#load-metric-select').addEventListener('change', event => { state.loadMetric = event.target.value; drawLoadChart(); });
$$('.monitor-sort-btn').forEach(button => button.addEventListener('click', () => {
  state.monitorSort = button.dataset.sort;
  $$('.monitor-sort-btn').forEach(item => item.classList.toggle('active', item === button));
  if (state.monitorPayload) renderMonitor(state.monitorPayload, false);
}));
window.addEventListener('resize', () => { if (state.currentView === 'monitor') drawMonitorChart(); if (state.currentView === 'load') drawLoadChart(); });
$('#stream-form').addEventListener('submit', saveStream);
$('#server-form').addEventListener('submit', saveServer);
$('#test-server-btn').addEventListener('click', testServerForm);
$('#inputs-form').addEventListener('submit', saveInputs);
$('#create-add-input').addEventListener('click', () => addInputRow($('#create-input-list'), '', true));
$('#edit-add-input').addEventListener('click', () => addInputRow($('#edit-input-list'), '', true));
$('#sync-stream-btn').addEventListener('click', syncCurrentStream);
$('#confirm-delete-btn').addEventListener('click', deleteCurrentStream);
$('#mobile-menu').addEventListener('click', () => $('.sidebar').classList.toggle('open'));
$$('.nav-item').forEach(item => item.addEventListener('click', () => switchView(item.dataset.view)));
$$('.modal-close').forEach(button => button.addEventListener('click', () => { const dialog=button.closest('dialog'); if(dialog.id==='preview-dialog') $('#preview-frame').src='about:blank'; dialog.close(); }));
$('#select-all-streams').addEventListener('change', e => { filteredStreams().forEach(x => e.target.checked ? state.selectedStreams.add(x.name) : state.selectedStreams.delete(x.name)); renderStreams(); updateBulkState(); });
$('#bulk-ondemand-btn').addEventListener('click', event => setStreamsMode([...state.selectedStreams], false, event.currentTarget));
$('#bulk-static-btn').addEventListener('click', event => setStreamsMode([...state.selectedStreams], true, event.currentTarget));
$('#bulk-disable-btn').addEventListener('click', event => setStreamsDisabled([...state.selectedStreams], true, event.currentTarget));
$('#bulk-enable-btn').addEventListener('click', event => setStreamsDisabled([...state.selectedStreams], false, event.currentTarget));
$('#bulk-open-btn').addEventListener('click', openBulk); $('#bulk-form').addEventListener('submit', saveBulk); $('#bulk-operation').addEventListener('change', updateBulkValueField);
$('#placement-enabled').addEventListener('change', togglePlacementMode); $('#placement-refresh-btn').addEventListener('click', loadPlacementOverview); $('#placement-filter').addEventListener('input', renderPlacementOverview); $('#placement-status-filter').addEventListener('change', renderPlacementOverview); $('#placement-assign-btn').addEventListener('click', () => assignPlacement('assigned')); $('#placement-mirror-btn').addEventListener('click', () => assignPlacement('mirror')); $('#placement-apply-btn').addEventListener('click', applyPlacement); $('#placement-select-all').addEventListener('change', event => { $$('.placement-row-check').forEach(box => { box.checked=event.target.checked; const name=box.closest('tr').dataset.name; event.target.checked ? state.placementSelected.add(name) : state.placementSelected.delete(name); }); updatePlacementSelection(); }); $('#stream-placement-mode').addEventListener('change', updateStreamPlacementFields); $('#stream-placement-server').addEventListener('change', updateStreamPlacementFields);
$('#cluster-refresh-btn').addEventListener('click', () => loadClusterOverview(true)); $('#cluster-settings-btn').addEventListener('click', openClusterSettings); $('#cluster-config-btn').addEventListener('click', toggleClusterConfig); $('#cluster-form').addEventListener('submit', saveClusterSettings);
$('#sync-refresh-btn').addEventListener('click', loadSyncOverview); $('#sync-filter').addEventListener('input', renderSyncOverview); $('#sync-status-filter').addEventListener('change', renderSyncOverview); $('#sync-selected-btn').addEventListener('click', syncSelected); $('#sync-select-all').addEventListener('change',e=>{$$('.sync-row-check').forEach(c=>c.checked=e.target.checked);updateSyncSelection();});
$('#sources-run-btn').addEventListener('click', runSourceChecks); $('#sources-filter').addEventListener('input', renderSourceChecks); $('#sources-status-filter').addEventListener('change', renderSourceChecks); $('#source-action-form').addEventListener('submit', saveSourceAction); $('#source-action-type').addEventListener('change', updateSourceActionFields);
$('#backups-refresh-btn').addEventListener('click', loadBackups); $('#backups-filter').addEventListener('input', renderBackups);
$('#audit-refresh-btn').addEventListener('click', loadAudit); $('#audit-filter').addEventListener('input', renderAudit);
$('#alerts-form').addEventListener('submit', saveAlerts); $('#alerts-test-btn').addEventListener('click', testAlert);
$$('.range-btn').forEach(b=>b.addEventListener('click',()=>{$$('.range-btn').forEach(x=>x.classList.toggle('active',x===b));state.monitorRange=b.dataset.range;if(state.monitorRange==='live'){state.monitorHistory=[];if(state.monitorPayload)renderMonitor(state.monitorPayload,true);}else loadMonitorHistory(state.monitorRange);}));
$$('.load-range-btn').forEach(button => button.addEventListener('click', () => {
  $$('.load-range-btn').forEach(item => item.classList.toggle('active', item === button));
  state.loadRange = button.dataset.range;
  if (state.loadRange === 'live') {
    state.loadHistory = [];
    if (state.loadPayload) renderLoad(state.loadPayload, true);
  } else loadLoadHistory(state.loadRange);
}));
$$('dialog').forEach(dialog => dialog.addEventListener('click', event => {
  const rect = dialog.getBoundingClientRect();
  const inside = event.clientX >= rect.left && event.clientX <= rect.right && event.clientY >= rect.top && event.clientY <= rect.bottom;
  if (!inside) dialog.close();
}));

init();
