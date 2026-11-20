// ============================================================================
// CLIENTE DO SERVIÇO INVENTORY
// ----------------------------------------------------------------------------
// O serviço Inventory será implementado na próxima entrega. Este módulo é só
// o cliente HTTP usado pelo Payment:
//   reserve() -> reserva e decrementa o stock ANTES da cobrança
//   release() -> ação de compensação, devolve os bilhetes se a cobrança falhar
//
// Enquanto INVENTORY_URL não estiver definido, as chamadas são ignoradas
// (no-op) e o Payment processa os pagamentos sem reservar stock.
//
// NOTA: os endpoints abaixo (/inventory/reserve e /inventory/release) são uma
// proposta de contrato; ajustar quando o Inventory for implementado.
// ============================================================================

const INVENTORY_URL = process.env.INVENTORY_URL || '';
const TIMEOUT_MS = parseInt(process.env.INVENTORY_TIMEOUT_MS || '2000', 10);

class InventoryError extends Error {
  constructor(code, message) {
    super(message);
    this.code = code; // 'OUT_OF_STOCK' | 'EVENT_NOT_FOUND' | 'UNAVAILABLE'
  }
}

async function post(path, body) {
  const controller = new AbortController();
  const timeoutId = setTimeout(() => controller.abort(), TIMEOUT_MS);
  try {
    return await fetch(`${INVENTORY_URL}${path}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(body),
      signal: controller.signal,
    });
  } catch (err) {
    throw new InventoryError('UNAVAILABLE', `Inventory indisponivel: ${err.message}`);
  } finally {
    clearTimeout(timeoutId);
  }
}

// Devolve { reservation_id } ou lança InventoryError
async function reserve(orderId, eventId, quantity) {
  if (!INVENTORY_URL) return { reservation_id: null, skipped: true };

  const res = await post('/inventory/reserve', {
    order_id: orderId,
    event_id: eventId,
    quantity,
  });

  if (res.status === 409) throw new InventoryError('OUT_OF_STOCK', 'Stock insuficiente.');
  if (res.status === 404) throw new InventoryError('EVENT_NOT_FOUND', 'Evento nao existe no inventario.');
  if (!res.ok) throw new InventoryError('UNAVAILABLE', `Inventory HTTP ${res.status}`);

  return await res.json();
}

// Compensação: devolve os bilhetes reservados ao Inventory
async function release(reservation) {
  if (!INVENTORY_URL || reservation.skipped) return;

  const res = await post('/inventory/release', { reservation_id: reservation.reservation_id });
  if (!res.ok) throw new InventoryError('UNAVAILABLE', `Inventory HTTP ${res.status}`);
}

module.exports = { reserve, release, InventoryError, enabled: Boolean(INVENTORY_URL) };
