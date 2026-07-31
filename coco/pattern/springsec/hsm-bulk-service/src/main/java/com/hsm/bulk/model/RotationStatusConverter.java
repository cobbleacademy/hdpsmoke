package com.hsm.bulk.model;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

import java.util.Locale;

/** Duplicated verbatim from com.hsm.core.model.RotationStatusConverter -- required for EdekRecord.rotationStatus to map to the lowercase values the shared edek_records table's CHECK constraint expects. */
@Converter(autoApply = true)
public class RotationStatusConverter implements AttributeConverter<RotationStatus, String> {

    @Override
    public String convertToDatabaseColumn(RotationStatus attribute) {
        return attribute == null ? null : attribute.name().toLowerCase(Locale.ROOT);
    }

    @Override
    public RotationStatus convertToEntityAttribute(String dbData) {
        return dbData == null ? null : RotationStatus.valueOf(dbData.toUpperCase(Locale.ROOT));
    }
}
