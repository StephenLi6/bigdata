package cn.itcast.pyg.report.cn.itcast.pyg.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("test")
public class TestWeb {

    //test/login
    @RequestMapping("login")
    public Map login(String username, String password) {

        System.out.println(username);
        System.out.println(password);
        HashMap<Object, Object> map = new HashMap<>();

        map.put("用户名", username);
        map.put("密码", password);
        return map;
    }
}
