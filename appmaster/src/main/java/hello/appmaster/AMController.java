package hello.appmaster;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/")
public class AMController {

    private final AppMaster appMaster;

    public AMController(AppMaster appMaster) {
        this.appMaster = appMaster;
    }

    @PostMapping
    public List<ContainerInfo> allocate(@RequestParam int count,
                                        @RequestParam int memory,
                                        @RequestParam(required = false, defaultValue = "1") int vCores) {

        return appMaster.submit(count, memory, vCores).stream().map(ContainerInfo::new).collect(Collectors.toList());
    }
}
