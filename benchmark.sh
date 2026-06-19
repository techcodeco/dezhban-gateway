#! testing gateway 
for i in {1..2000}; do
  curl -X POST http://localhost:3000/webhook/test -H "Content-Type: application/json" -d '{"update":{"type": "NewMessage","chat_id": "g0Gjev40fd66dad3d515627defc8193b","new_message": {"message_id": "1793907667383746705","text": "چون خیلی ارادت دارم به همسایه همون ارادت خاصی دارم ۴۷۵ تومن 😂😂","reply_to_message_id": "1793905666649746705","time": "1781622117","is_edited": false,"sender_type": "User","sender_id": "u0CGSG6055b33bd2be4e5981a9944082"},"update_time": 1781622117}}' &
  if [ $((i % 100)) -eq 0 ]; then
    wait
    echo "Sent $i requests"
  fi
done
wait