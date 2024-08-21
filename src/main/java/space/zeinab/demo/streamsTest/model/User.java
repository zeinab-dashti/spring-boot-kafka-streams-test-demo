package space.zeinab.demo.streamsTest.model;

import java.time.LocalDateTime;

public record User(String userId, String name, String address, LocalDateTime modifiedTime) {
}