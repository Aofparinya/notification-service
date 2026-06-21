package main

import "testing"

func TestRender(t *testing.T) {
	got, err := render("Order {{.orderNumber}}", map[string]any{"orderNumber": "ORD-1"})
	if err != nil || got != "Order ORD-1" {
		t.Fatal(got, err)
	}
}
