-- این خط به wrk می‌گوید که متد درخواست POST است
wrk.method = "POST"

-- اینجا بدنه درخواست (body) را به صورت JSON می‌نویسیم
wrk.body = '{"update":{"message":"hello world"}}'
-- "update":{"message":"hello world"}
-- این خط هم هدر (header) را تنظیم می‌کند تا سرور بفهمد که داده از نوع JSON است
wrk.headers["Content-Type"] = "application/json"
