"""Exact decimal storage shared with the migrated database's TEXT columns."""

from decimal import Decimal, localcontext

from sqlalchemy.types import Text, TypeDecorator


class ExactNumeric(TypeDecorator):
    impl = Text
    cache_ok = True

    def __init__(self, precision=None, scale=None, **kwargs):
        super().__init__(**kwargs)
        self.precision = precision
        self.scale = scale

    def process_bind_param(self, value, dialect):
        if value is None:
            return None
        if isinstance(value, float):
            raise TypeError('Exact numeric values must not pass through float')
        value = Decimal(value)
        if not value.is_finite():
            raise ValueError('Exact numeric values must be finite')
        return format(value, 'f')

    def process_result_value(self, value, dialect):
        if value is None:
            return None
        value = Decimal(value)
        if self.scale is not None:
            # Restore the PostgreSQL numeric scale for existing string responses.
            with localcontext() as context:
                context.prec = max(self.precision or 0, len(value.as_tuple().digits),
                                   value.adjusted() + self.scale + 1, 28)
                value = value.quantize(Decimal(1).scaleb(-self.scale))
        return value
