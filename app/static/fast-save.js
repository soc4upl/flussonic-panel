/*
 * Cyrius Stream Control v5.13.2 Fast Save compatibility patch.
 *
 * Goal:
 * - keep backend backup/audit/write flow unchanged;
 * - avoid full /api/streams?server_id=all reload after a successful stream/input save;
 * - preserve aggregate ALL-view metadata already present in state.
 */

(function installFastSave() {
  function mergeAllViewStream(existing, fallback, normalized, selectedResult) {
    const source = fallback || normalized || existing;
    if (!source || !source.name) return null;

    const merged = {
      ...(existing || {}),
      ...(normalized || {}),
      ...(fallback || {}),
    };

    // Preserve aggregate ALL-view metadata unless the caller explicitly supplied it.
    for (const key of [
      'server_ids',
      'server_names',
      'server_count',
      'reference_server_id',
      'reference_server_name',
    ]) {
      if (existing && existing[key] !== undefined && (fallback == null || fallback[key] === undefined)) {
        merged[key] = existing[key];
      }
    }

    // Runtime flags returned by PUT can be incomplete on some Flussonic versions.
    // If they are absent, keep the values already shown in the ALL table.
    for (const key of ['alive', 'running', 'status', 'disabled', 'named_by']) {
      if (
        existing &&
        (normalized == null || normalized[key] === undefined) &&
        (fallback == null || fallback[key] === undefined)
      ) {
        merged[key] = existing[key];
      }
    }

    // If a successful result came from a concrete CDN, keep/repair its membership
    // in the aggregate row. We do not remove other CDN memberships here because
    // ordinary PATCH/input update does not delete stream copies.
    if (selectedResult?.server_id) {
      const ids = new Set(merged.server_ids || existing?.server_ids || []);
      ids.add(selectedResult.server_id);
      merged.server_ids = [...ids];

      const server = (state.servers || []).find(item => item.id === selectedResult.server_id);
      const names = new Set(merged.server_names || existing?.server_names || []);
      if (server?.name) names.add(server.name);
      merged.server_names = [...names];
      merged.server_count = merged.server_ids.length;

      if (!merged.reference_server_id) {
        merged.reference_server_id = selectedResult.server_id;
        merged.reference_server_name = server?.name || selectedResult.server_name || null;
      }
    }

    return merged;
  }

  const originalApplyMutationResult = applyMutationResult;

  applyMutationResult = async function fastApplyMutationResult(result, streamName, fallbackStream = null) {
    if (state.selectedServerId !== 'all') {
      return originalApplyMutationResult(result, streamName, fallbackStream);
    }

    const existing = state.streams.find(item => item.name === streamName) || null;
    const selectedResult =
      successfulResultForSelectedServer(result, streamName) ||
      (result?.results || []).find(item => item.ok) ||
      null;

    if (!selectedResult) return false;

    const normalized = selectedResult.data?.normalized_stream || null;
    const merged = mergeAllViewStream(existing, fallbackStream, normalized, selectedResult);

    if (merged?.name) {
      replaceStreamInState(merged);
      return true;
    }

    // Safety fallback: preserve previous behavior if we cannot confidently
    // construct a local row.
    await loadStreams();
    return true;
  };

  console.info('[Cyrius] Fast Save v5.13.2 enabled');
})();
