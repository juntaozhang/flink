package org.apache.flink.streaming.examples.my.kafka;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UserEvent {
    private String eventType;
    private Integer userId;
    private Long eventTime;

    public static UserEvent click(int eventTime) {
        return new UserEvent("click", eventTime % 5, (long) eventTime);
    }

    public static UserEvent watermark(int id) {
        return new UserEvent("watermark", null, (long) id);
    }
}
