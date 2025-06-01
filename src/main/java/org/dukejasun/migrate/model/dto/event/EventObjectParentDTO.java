package org.dukejasun.migrate.model.dto.event;

import org.dukejasun.migrate.queue.model.EventObject;
import org.dukejasun.migrate.utils.JacksonUtil;

/**
 * @author dukedpsun
 */
public abstract class EventObjectParentDTO implements EventObject {
    private static final long serialVersionUID = 6089842466293487030L;

    public void setType(String type) {
    }

    @Override
    public String toString() {
        return JacksonUtil.toJson(this);
    }
}
