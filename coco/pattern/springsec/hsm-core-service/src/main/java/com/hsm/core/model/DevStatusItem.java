package com.hsm.core.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import java.time.OffsetDateTime;

/**
 * Demo-only. A row in the Development Status tab -- one tracked item (a
 * shipped component or an open backlog entry) with an N/P/C status. Backed
 * by the DB (not a static file) so edits made in the UI survive restarts.
 */
@Entity
@Table(name = "dev_status_items")
public class DevStatusItem {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "category", nullable = false, length = 64)
    private String category;

    @Column(name = "item", nullable = false, length = 512)
    private String item;

    @Column(name = "status", nullable = false, length = 1) // "N" | "P" | "C"
    private String status;

    @Column(name = "notes", length = 1024)
    private String notes;

    @Column(name = "updated_at")
    private OffsetDateTime updatedAt;

    protected DevStatusItem() {
        // JPA
    }

    public DevStatusItem(String category, String item, String status, String notes) {
        this.category = category;
        this.item = item;
        this.status = status;
        this.notes = notes;
        this.updatedAt = OffsetDateTime.now();
    }

    public Long getId() {
        return id;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public String getItem() {
        return item;
    }

    public void setItem(String item) {
        this.item = item;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public String getNotes() {
        return notes;
    }

    public void setNotes(String notes) {
        this.notes = notes;
    }

    public OffsetDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(OffsetDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
}
