from psqlgraph import Edge, pg_property


class ShippedTo(object):
    __label__ = 'shipped_to'


class AliquotShippedToCenter(ShippedTo, Edge):
    __src_class__ = 'Aliquot'
    __dst_class__ = 'Center'
    __src_dst_assoc__ = 'centers'
    __dst_src_assoc__ = 'aliquots'

    @pg_property
    def plate_id(self, value):
        self._props['plate_id'] = value

    @pg_property
    def plate_row(self, value):
        self._props['plate_row'] = value

    @pg_property
    def plate_column(self, value):
        self._props['plate_column'] = value

    @pg_property
    def shipment_datetime(self, value):
        self._props['shipment_datetime'] = value

    @pg_property
    def shipment_reason(self, value):
        self._props['shipment_reason'] = value

    @pg_property
    def shipment_center_id(self, value):
        self._props['shipment_center_id'] = value


class PortionShippedToCenter(Edge, ShippedTo):
    __src_class__ = 'Portion'
    __dst_class__ = 'Center'
    __src_dst_assoc__ = 'centers'
    __dst_src_assoc__ = 'portions'

    @pg_property
    def plate_id(self, value):
        self._props['plate_id'] = value

    @pg_property
    def plate_row(self, value):
        self._props['plate_row'] = value

    @pg_property
    def plate_column(self, value):
        self._props['plate_column'] = value

    @pg_property
    def shipment_datetime(self, value):
        self._props['shipment_datetime'] = value

    @pg_property
    def shipment_reason(self, value):
        self._props['shipment_reason'] = value

    @pg_property
    def shipment_center_id(self, value):
        self._props['shipment_center_id'] = value
