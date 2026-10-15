package main

import (
    "fmt"
    "os"
    "github.com/banbox/banbot/orm/ormo"
)

func main() {
    if len(os.Args) != 2 { panic("usage: orderinspect file") }
    orders, err := ormo.LoadOrdersGob(os.Args[1]); if err != nil { panic(err) }
    fmt.Printf("count=%d\n", len(orders))
    for _, i := range []int{5610, 5611, 5612, 5613, 5614, 5615, 5616, 5617, 5618, 5619, 5620, 5621, 5622, 5623, 5624, 5625, 5630, len(orders)-1} {
        if i < 0 || i >= len(orders) { continue }
        o := orders[i]
        if o == nil || o.IOrder == nil { fmt.Printf("%d nil\n", i); continue }
        var exitPrice float64
        if o.Exit != nil { exitPrice = o.Exit.Price }
        fmt.Printf("%d symbol=%s tf=%s short=%v enter=%d exit=%d profit_rate=%.12g profit=%.12g enter_price=%.12g exit_price=%.12g enter_tag=%s exit_tag=%s strategy=%s\n", i, o.Symbol, o.Timeframe, o.Short, o.RealEnterMS(), o.RealExitMS(), o.ProfitRate, o.Profit, o.Enter.Price, exitPrice, o.EnterTag, o.ExitTag, o.Strategy)
    }
}
