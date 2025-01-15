package ru.test;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

@RestController
@RequiredArgsConstructor
public class TestController {

    private final KafkaProducerService kafkaProducerService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostMapping("/send")
    public void sendMessage(@RequestBody TestDataClass message) {

        String s = null;
        try {
            s = objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException e) {

        }

        if (s != null) {
            kafkaProducerService.sendMessage("test-topic", s);
        }
    }
}
