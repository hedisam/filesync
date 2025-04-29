package psurls

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	ObjectKey      = "key"
	SHA256Checksum = "sha256"
	Size           = "size"
	MTime          = "mtime"
	Expiry         = "exp"
	AccessKeyID    = "aki"
	Signature      = "sig"
)

var (
	ErrURLExpired        = errors.New("url expired")
	ErrSignatureMismatch = errors.New("signature mismatch")
)

type URLData struct {
	ObjectKey      string
	SHA256Checksum string
	Size           int64
	MTime          int64
	Expiry         int64
	AccessKeyID    string
}

func Generate(data URLData, baseURL, secretKey string) (string, error) {
	var qValues url.Values = map[string][]string{
		ObjectKey:      {data.ObjectKey},
		SHA256Checksum: {data.SHA256Checksum},
		Size:           {strconv.FormatInt(data.Size, 10)},
		MTime:          {strconv.FormatInt(data.Expiry, 10)},
		Expiry:         {strconv.FormatInt(data.Expiry, 10)},
		AccessKeyID:    {data.AccessKeyID},
	}

	sigData := prepareSigData(qValues)
	sigBytes := sign(sigData, secretKey)
	sig := hex.EncodeToString(sigBytes)
	qValues.Set(Signature, sig)

	return fmt.Sprintf("%s?%s", baseURL, qValues.Encode()), nil
}

func Validate(values url.Values, secretKey string) (URLData, error) {
	exp, err := strconv.ParseInt(values.Get(Expiry), 10, 64)
	if err != nil {
		return URLData{}, fmt.Errorf("invalid or missing expiry: %w", err)
	}
	if time.Now().UTC().Unix() > exp {
		return URLData{}, ErrURLExpired
	}

	providedSig := values.Get(Signature)
	if providedSig == "" {
		return URLData{}, fmt.Errorf("missing signature")
	}
	providedSigBytes, err := hex.DecodeString(providedSig)
	if err != nil {
		return URLData{}, fmt.Errorf("invalid signature encoding: %w", err)
	}

	data := prepareSigData(values)
	expectedSigBytes := sign(data, secretKey)

	if !hmac.Equal(expectedSigBytes, providedSigBytes) {
		return URLData{}, ErrSignatureMismatch
	}

	size, err := strconv.ParseInt(values.Get(Size), 10, 64)
	if err != nil {
		return URLData{}, fmt.Errorf("invalid or missing size: %w", err)
	}
	mtime, err := strconv.ParseInt(values.Get(MTime), 10, 64)
	if err != nil {
		return URLData{}, fmt.Errorf("invalid or missing mtime: %w", err)
	}

	return URLData{
		ObjectKey:      values.Get(ObjectKey),
		SHA256Checksum: values.Get(SHA256Checksum),
		Size:           size,
		MTime:          mtime,
		Expiry:         exp,
		AccessKeyID:    values.Get(AccessKeyID),
		//Signature:      hex.EncodeToString(expectedSigBytes),
	}, nil
}

func prepareSigData(values url.Values) string {
	// sorting the keys is required for a deterministic signature hash when generating and then validating the signature
	keys := slices.Collect(maps.Keys(values))
	sort.Strings(keys)

	data := &strings.Builder{}
	for k := range slices.Values(keys) {
		if k == Signature {
			// the signature shouldn't be included in the data when re-calculating the signature hash
			continue
		}
		val := ""
		if len(values[k]) > 0 {
			val = values[k][0]
		}
		data.WriteString(fmt.Sprintf("%s=%s\n", k, val))
	}
	return data.String()
}

func sign(data, secretKey string) []byte {
	mac := hmac.New(sha256.New, []byte(secretKey))
	mac.Write([]byte(data))
	return mac.Sum(nil)
}
