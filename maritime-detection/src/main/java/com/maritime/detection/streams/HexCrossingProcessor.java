package com.maritime.detection.streams;

import com.maritime.common.dto.EnrichedVesselEvent;
import com.maritime.common.dto.HexCrossingEvent;
import com.maritime.common.dto.VesselEvent;
import com.maritime.common.geo.GeoUtils;
import org.apache.kafka.streams.processor.api.Processor;
import org.apache.kafka.streams.processor.api.ProcessorContext;
import org.apache.kafka.streams.processor.api.Record;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Kafka Streams processor that detects when a vessel crosses from one
 * H3 resolution-7 cell (~15 km diameter) to another.
 *
 * <p>Maintains a {@code KeyValueStore<String, String>} mapping each MMSI to
 * its last known H3 cell address. On every incoming {@link EnrichedVesselEvent},
 * the processor resolves the vessel's current cell and compares it against the
 * stored cell. If they differ, a {@link HexCrossingEvent} is forwarded
 * downstream; if they match, no record is emitted (edge-triggered).
 */
public class HexCrossingProcessor
        implements Processor<String, EnrichedVesselEvent, String, HexCrossingEvent> {

    static final String HEX_STATE_STORE = "hex-cell-store";

    private ProcessorContext<String, HexCrossingEvent> ctx;
    private KeyValueStore<String, String> cellStore;

    @Override
    public void init(ProcessorContext<String, HexCrossingEvent> context) {
        this.ctx       = context;
        this.cellStore = context.getStateStore(HEX_STATE_STORE);
    }

    @Override
    public void process(Record<String, EnrichedVesselEvent> record) {
        EnrichedVesselEvent event = record.value();
        if (event == null) return;

        VesselEvent ve     = event.getVesselEvent();
        String mmsi        = ve.getMmsi();
        double lat         = ve.getLatitude();
        double lon         = ve.getLongitude();
        String currentCell = GeoUtils.latLonToH3Cell(lat, lon);
        String previousCell = cellStore.get(mmsi);

        cellStore.put(mmsi, currentCell);

        if (previousCell != null && !previousCell.equals(currentCell)) {
            HexCrossingEvent crossing = HexCrossingEvent.newBuilder()
                    .setMmsi(mmsi)
                    .setFromCell(previousCell)
                    .setToCell(currentCell)
                    .setLatitude(lat)
                    .setLongitude(lon)
                    .setSpeed(ve.getSpeed())
                    .setSourceType(ve.getSourceType())
                    .setTimestamp(ve.getTimestamp())
                    .build();
            ctx.forward(record.withValue(crossing));
        }
    }
}
