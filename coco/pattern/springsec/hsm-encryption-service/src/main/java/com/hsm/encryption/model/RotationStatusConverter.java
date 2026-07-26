package com.hsm.encryption.model;

import jakarta.persistence.AttributeConverter;
import jakarta.persistence.Converter;

import java.util.Locale;

/**
 * Column stores lowercase values ('current' | 'pending' | 'rotated') to match the
 * CHECK constraint in db/migration/V1__initial_schema.sql, while the Java enum
 * follows standard uppercase constant naming.
 */
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
