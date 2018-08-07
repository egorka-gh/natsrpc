package main

//BonusReq is the bonus request data
type BonusReq struct {
	Accaunt string
	Total   float32
}

//BonusResp is the bonus responce data
type BonusResp struct {
	Accaunt  string
	NewTotal float32
	Discont  float32
	Caption  string
}

//CallError is the rpc error data
type CallError struct {
	Code    int
	Message string
}
