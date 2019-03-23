package cn.itcast.pyg.report.cn.itcast.pyg.controller;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;

@Controller
@RequestMapping("test")
public class TestWeb {

    //test/login
    @RequestMapping("login")
    public void login(String username, String password) {
        //TODO 完善login方法
        System.out.println(username);
        System.out.println(password);
    }
}
