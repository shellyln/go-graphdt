package datatable

import (
	"errors"

	"github.com/shellyln/go-graphdt/datatable/datacolimpl"
	"github.com/shellyln/go-graphdt/datatable/datacolumn"
	. "github.com/shellyln/go-graphdt/datatable/types"
	"github.com/shellyln/go-graphdt/nameutil"
)

type JoinOptions struct {
	RightMustZeroOrOnce bool
	// TODO: IsObjectGraph bool
}

// Left join by colL and colR; colL must be primary key and colR must be foreign key.
func (p *DataTable) LeftJoin(colL int, dtR *DataTable, colR int, relName string, options JoinOptions) (*DataTable, error) {
	var extendLen int
	leftDict := make(map[interface{}]joinLeftDictValue)
	leftKeyRelDict := make(map[joinLeftKeyAndRelPair]struct{})

	leftLen := p.Len()
	leftColsLen := len(p.cols)
	rawLeftIndex := p.index.GetRawValues()

	var rawColLRelVec []int
	if p.header[colL].relIndex >= 0 {
		rawColLRelVec = p.relations[p.header[colL].relIndex].vec.GetRawValues()
	}

	for i, col := 0, p.cols[colL]; i < leftLen; i++ {
		v := col.GetAny(rawLeftIndex[i])
		if v == nil || col.IsNull(rawLeftIndex[i]) {
			continue
		}
		if dictLeaf, ok := leftDict[v]; ok {
			dictLeaf.count++

			relVal := -1
			if rawColLRelVec != nil {
				relVal = rawColLRelVec[rawLeftIndex[i]]
			}
			keyAndRel := joinLeftKeyAndRelPair{key: v, rel: relVal}
			if _, ok := leftKeyRelDict[keyAndRel]; !ok {
				leftKeyRelDict[keyAndRel] = struct{}{}
				dictLeaf.numOfUniqueRel++
			}

			leftDict[v] = dictLeaf
		} else {
			leftDict[v] = joinLeftDictValue{
				count:          1,
				leftIndex:      i,
				numOfUniqueRel: 1,
			}

			relVal := -1
			if rawColLRelVec != nil {
				relVal = rawColLRelVec[rawLeftIndex[i]]
			}
			leftKeyRelDict[joinLeftKeyAndRelPair{key: v, rel: relVal}] = struct{}{}
		}
	}

	dtR = dtR.Borrow()
	if err := dtR.Sort(SortInfo{Col: colR}); err != nil {
		return nil, err
	}

	rightLen := dtR.Len()
	rightColsLen := len(dtR.cols)
	rawRightIndex := dtR.index.GetRawValues()
	rightCount := 0

	for i, col := 0, dtR.cols[colR]; i < rightLen; i++ {
		v := col.GetAny(rawRightIndex[i])
		if v == nil || col.IsNull(rawRightIndex[i]) {
			continue
		}
		if dictLeaf, ok := leftDict[v]; ok {
			rightCount += dictLeaf.count
			dictLeaf.rightIndexS = i
			dictLeaf.rightIndexE = i + 1
			for j := i + 1; j < rightLen; j++ {
				if col.GetAny(rawRightIndex[j]) == v {
					if options.RightMustZeroOrOnce {
						return nil, errors.New("DataTable:LeftJoin failed: Many right records found")
					}
					rightCount += dictLeaf.count
					dictLeaf.rightIndexE = j + 1
					extendLen += dictLeaf.count
					i++
				} else {
					break
				}
			}
			leftDict[v] = dictLeaf
		}
	}

	retDt := *p.initJoinResultDt(colL, dtR, colR, relName, extendLen, options, false)

	leftRelsLen := len(p.relations)
	leftRelsIncl := 0
	if leftRelsLen == 0 {
		leftRelsIncl = 1
	}
	rightRelsLen := len(dtR.relations)
	rawTopRelVec := retDt.relations[leftRelsLen+leftRelsIncl+rightRelsLen].vec.GetRawValues()

	leftSrcRowMap := make([]int, 0, leftLen+extendLen)
	rightSrcRowMap := make([]int, 0, rightCount)
	rightDstRowMap := make([]int, 0, rightCount)

	for i, ext := 0, 0; i < leftLen; i++ {
		leftSrcRowMap = leftSrcRowMap[:len(leftSrcRowMap)+1]

		leftSrcRowMap[i+ext] = rawLeftIndex[i]

		rightRelsNull := false

		v := p.cols[colL].GetAny(rawLeftIndex[i])
		if v == nil || p.cols[colL].IsNull(rawLeftIndex[i]) {
			rightRelsNull = true
		} else {
			if dictLeaf, ok := leftDict[v]; ok {
				rightSegLen := dictLeaf.rightIndexE - dictLeaf.rightIndexS

				if rightSegLen > 0 {
					rightRowMapStart := len(rightSrcRowMap)

					leftSrcRowMap = leftSrcRowMap[:len(leftSrcRowMap)+(rightSegLen-1)]
					rightSrcRowMap = rightSrcRowMap[:rightRowMapStart+rightSegLen]
					rightDstRowMap = rightDstRowMap[:rightRowMapStart+rightSegLen]

					for j := dictLeaf.rightIndexS; j < dictLeaf.rightIndexE; j++ {
						currRightSegCount := j - dictLeaf.rightIndexS

						k := i + ext + currRightSegCount

						if currRightSegCount > 0 {
							leftSrcRowMap[k] = rawLeftIndex[i]
						}

						rightSrcRowMap[rightRowMapStart+currRightSegCount] = rawRightIndex[j]
						rightDstRowMap[rightRowMapStart+currRightSegCount] = k

						rightRepeatCount := rightSegLen*dictLeaf.used + currRightSegCount

						// NOTE: Delete surplus object graph relationships. This is a duplicate record when walk.
						if dictLeaf.numOfUniqueRel*rightSegLen <= rightRepeatCount {
							rawTopRelVec[k] = -1

							// TODO: If options.IsObjectGraph is set:
							//         Set null to right columns; resize `rightSrcRowMap` and `rightDstRowMap`, adjust write index of them in this loop.
							//         If currRightSegCount > 0, shrink result table length.
						}
					}
					ext += rightSegLen - 1
				} else {
					rightRelsNull = true
				}

				dictLeaf.used++
				leftDict[v] = dictLeaf
			} else {
				// NOTE: Maybe not reached here.
				rightRelsNull = true
			}
		}
		if rightRelsNull {
			for c := 0; c <= rightRelsLen; c++ {
				retDt.relations[leftRelsLen+leftRelsIncl+c].vec.Set(i+ext, -1)
			}
		}
	}

	for c := 0; c < leftColsLen; c++ {
		retDt.cols[c].FillByRowMap(nil, p.cols[c], leftSrcRowMap)
	}
	for c := 0; c < rightColsLen; c++ {
		retDt.cols[leftColsLen+c].FillByRowMap(rightDstRowMap, dtR.cols[c], rightSrcRowMap)
	}
	for c := 0; c < leftRelsLen+leftRelsIncl; c++ {
		if c < len(p.relations) {
			retDt.relations[c].vec.FillByRowMap(nil, p.relations[c].vec, leftSrcRowMap)
		} else {
			break
		}
	}
	if leftRelsIncl > 0 {
		retDt.relations[leftRelsLen].vec.FillByRowMap(nil, p.index, leftSrcRowMap) // TODO: BUG: index out of range if new with non zero size and AppendFromCSV
	}
	for c := 0; c < rightRelsLen; c++ {
		retDt.relations[leftRelsLen+leftRelsIncl+c].vec.FillByRowMap(rightDstRowMap, dtR.relations[c].vec, rightSrcRowMap)
	}

	return &retDt, nil
}

// Inner join by colL and colR; colL must be primary key and colR must be foreign key.
func (p *DataTable) InnerJoin(colL int, dtR *DataTable, colR int, relName string, options JoinOptions) (*DataTable, error) {
	var extendLen int
	leftDict := make(map[interface{}]joinLeftDictValue)
	leftKeyRelDict := make(map[joinLeftKeyAndRelPair]struct{})

	leftLen := p.Len()
	leftColsLen := len(p.cols)
	rawLeftIndex := p.index.GetRawValues()

	var rawColLRelVec []int
	if p.header[colL].relIndex >= 0 {
		rawColLRelVec = p.relations[p.header[colL].relIndex].vec.GetRawValues()
	}

	for i, col := 0, p.cols[colL]; i < leftLen; i++ {
		v := col.GetAny(rawLeftIndex[i])
		if v == nil || col.IsNull(rawLeftIndex[i]) {
			extendLen--
			continue
		}
		if dictLeaf, ok := leftDict[v]; ok {
			dictLeaf.count++

			relVal := -1
			if rawColLRelVec != nil {
				relVal = rawColLRelVec[rawLeftIndex[i]]
			}
			keyAndRel := joinLeftKeyAndRelPair{key: v, rel: relVal}
			if _, ok := leftKeyRelDict[keyAndRel]; !ok {
				leftKeyRelDict[keyAndRel] = struct{}{}
				dictLeaf.numOfUniqueRel++
			}

			leftDict[v] = dictLeaf
		} else {
			leftDict[v] = joinLeftDictValue{
				count:          1,
				leftIndex:      i,
				numOfUniqueRel: 1,
			}

			relVal := -1
			if rawColLRelVec != nil {
				relVal = rawColLRelVec[rawLeftIndex[i]]
			}
			leftKeyRelDict[joinLeftKeyAndRelPair{key: v, rel: relVal}] = struct{}{}
		}
	}

	dtR = dtR.Borrow()
	if err := dtR.Sort(SortInfo{Col: colR}); err != nil {
		return nil, err
	}

	rightLen := dtR.Len()
	rightColsLen := len(dtR.cols)
	rawRightIndex := dtR.index.GetRawValues()

	for i, col := 0, dtR.cols[colR]; i < rightLen; i++ {
		v := col.GetAny(rawRightIndex[i])
		if v == nil || col.IsNull(rawRightIndex[i]) {
			// NOTE: Don't declement `extendLen`
			continue
		}
		if dictLeaf, ok := leftDict[v]; ok {
			dictLeaf.rightIndexS = i
			dictLeaf.rightIndexE = i + 1
			for j := i + 1; j < rightLen; j++ {
				if col.GetAny(rawRightIndex[j]) == v {
					if options.RightMustZeroOrOnce {
						return nil, errors.New("DataTable:InnerJoin failed: Many right records found")
					}
					dictLeaf.rightIndexE = j + 1
					extendLen += dictLeaf.count
					i++
				} else {
					break
				}
			}
			leftDict[v] = dictLeaf
		} else {
			continue
		}
	}
	for _, dictLeaf := range leftDict {
		if dictLeaf.rightIndexS == dictLeaf.rightIndexE {
			extendLen -= dictLeaf.count
		}
	}

	retDt := *p.initJoinResultDt(colL, dtR, colR, relName, extendLen, options, true)

	leftRelsLen := len(p.relations)
	leftRelsIncl := 0
	if leftRelsLen == 0 {
		leftRelsIncl = 1
	}
	rightRelsLen := len(dtR.relations)
	rawTopRelVec := retDt.relations[leftRelsLen+leftRelsIncl+rightRelsLen].vec.GetRawValues()

	leftSrcRowMap := make([]int, 0, leftLen+extendLen)
	rightSrcRowMap := make([]int, 0, leftLen+extendLen)

	for i, ext := 0, 0; i < leftLen; i++ {
		v := p.cols[colL].GetAny(rawLeftIndex[i])
		if v == nil || p.cols[colL].IsNull(rawLeftIndex[i]) {
			ext--
			continue
		}

		if dictLeaf, ok := leftDict[v]; ok && dictLeaf.rightIndexE-dictLeaf.rightIndexS > 0 {
			rightSegLen := dictLeaf.rightIndexE - dictLeaf.rightIndexS

			leftSrcRowMap = leftSrcRowMap[:len(leftSrcRowMap)+rightSegLen]
			rightSrcRowMap = rightSrcRowMap[:len(rightSrcRowMap)+rightSegLen]

			for j := dictLeaf.rightIndexS; j < dictLeaf.rightIndexE; j++ {
				currRightSegCount := j - dictLeaf.rightIndexS

				k := i + ext + currRightSegCount

				leftSrcRowMap[k] = rawLeftIndex[i]
				rightSrcRowMap[k] = rawRightIndex[j]

				rightRepeatCount := rightSegLen*dictLeaf.used + currRightSegCount

				// NOTE: Delete surplus object graph relationships. This is a duplicate record when walk.
				if dictLeaf.numOfUniqueRel*rightSegLen <= rightRepeatCount {
					rawTopRelVec[k] = -1

					// TODO: If options.IsObjectGraph is set:
					//         Set null to right columns; resize `rightSrcRowMap` and `rightDstRowMap`, adjust write index of them in this loop.
					//         If currRightSegCount > 0, shrink result table length.
				}
			}
			ext += rightSegLen - 1

			dictLeaf.used++
			leftDict[v] = dictLeaf
		} else {
			ext--
			continue
		}
	}

	for c := 0; c < leftColsLen; c++ {
		retDt.cols[c].FillByRowMap(nil, p.cols[c], leftSrcRowMap)
	}
	for c := 0; c < rightColsLen; c++ {
		retDt.cols[leftColsLen+c].FillByRowMap(nil, dtR.cols[c], rightSrcRowMap)
	}
	for c := 0; c < leftRelsLen+leftRelsIncl; c++ {
		if c < len(p.relations) {
			retDt.relations[c].vec.FillByRowMap(nil, p.relations[c].vec, leftSrcRowMap)
		} else {
			break
		}
	}
	if leftRelsIncl > 0 {
		retDt.relations[leftRelsLen].vec.FillByRowMap(nil, p.index, leftSrcRowMap)
	}
	for c := 0; c < rightRelsLen; c++ {
		retDt.relations[leftRelsLen+leftRelsIncl+c].vec.FillByRowMap(nil, dtR.relations[c].vec, rightSrcRowMap)
	}

	return &retDt, nil
}

// Full join by colL and colR; colL must be primary key and colR must be foreign key.
func (p *DataTable) FullJoin(colL int, dtR *DataTable, colR int, relName string, options JoinOptions) (*DataTable, error) {
	var extendLen int
	leftDict := make(map[interface{}]joinLeftDictValue)
	leftKeyRelDict := make(map[joinLeftKeyAndRelPair]struct{})

	leftLen := p.Len()
	leftColsLen := len(p.cols)
	rawLeftIndex := p.index.GetRawValues()
	leftCount := leftLen

	var rawColLRelVec []int
	if p.header[colL].relIndex >= 0 {
		rawColLRelVec = p.relations[p.header[colL].relIndex].vec.GetRawValues()
	}

	for i, col := 0, p.cols[colL]; i < leftLen; i++ {
		v := col.GetAny(rawLeftIndex[i])
		if v == nil || col.IsNull(rawLeftIndex[i]) {
			continue
		}
		if dictLeaf, ok := leftDict[v]; ok {
			dictLeaf.count++

			relVal := -1
			if rawColLRelVec != nil {
				relVal = rawColLRelVec[rawLeftIndex[i]]
			}
			keyAndRel := joinLeftKeyAndRelPair{key: v, rel: relVal}
			if _, ok := leftKeyRelDict[keyAndRel]; !ok {
				leftKeyRelDict[keyAndRel] = struct{}{}
				dictLeaf.numOfUniqueRel++
			}

			leftDict[v] = dictLeaf
		} else {
			leftDict[v] = joinLeftDictValue{
				count:          1,
				leftIndex:      i,
				numOfUniqueRel: 1,
			}

			relVal := -1
			if rawColLRelVec != nil {
				relVal = rawColLRelVec[rawLeftIndex[i]]
			}
			leftKeyRelDict[joinLeftKeyAndRelPair{key: v, rel: relVal}] = struct{}{}
		}
	}

	dtR = dtR.Borrow()
	if err := dtR.Sort(SortInfo{Col: colR}); err != nil {
		return nil, err
	}

	rightLen := dtR.Len()
	rightColsLen := len(dtR.cols)
	rawRightIndex := dtR.index.GetRawValues()
	rightCount := dtR.Len()

	rightExtra := make([]int, 0, datacolimpl.DataColumnImpl_DefaultSize)

	for i, col := 0, dtR.cols[colR]; i < rightLen; i++ {
		v := col.GetAny(rawRightIndex[i])
		if v == nil || col.IsNull(rawRightIndex[i]) {
			extendLen++
			rightExtra = append(rightExtra, i)
			continue
		}
		if dictLeaf, ok := leftDict[v]; ok {
			rightCount += dictLeaf.count - 1
			dictLeaf.rightIndexS = i
			dictLeaf.rightIndexE = i + 1
			for j := i + 1; j < rightLen; j++ {
				if col.GetAny(rawRightIndex[j]) == v {
					if options.RightMustZeroOrOnce {
						return nil, errors.New("DataTable:FullJoin failed: Many right records found")
					}
					rightCount += dictLeaf.count - 1
					leftCount += dictLeaf.count
					dictLeaf.rightIndexE = j + 1
					extendLen += dictLeaf.count
					i++
				} else {
					break
				}
			}
			leftDict[v] = dictLeaf
		} else {
			extendLen++
			rightExtra = append(rightExtra, i)
			continue
		}
	}

	retDt := *p.initJoinResultDt(colL, dtR, colR, relName, extendLen, options, false)

	leftRelsLen := len(p.relations)
	leftRelsIncl := 0
	if leftRelsLen == 0 {
		leftRelsIncl = 1
	}
	rightRelsLen := len(dtR.relations)
	rawTopRelVec := retDt.relations[leftRelsLen+leftRelsIncl+rightRelsLen].vec.GetRawValues()

	leftSrcRowMap := make([]int, 0, leftCount)
	rightSrcRowMap := make([]int, 0, rightCount)
	rightDstRowMap := make([]int, 0, rightCount)

	ext := 0
	for i := 0; i < leftLen; i++ {
		leftSrcRowMap = leftSrcRowMap[:len(leftSrcRowMap)+1]

		leftSrcRowMap[i+ext] = rawLeftIndex[i]

		rightRelsNull := false

		v := p.cols[colL].GetAny(rawLeftIndex[i])
		if v == nil || p.cols[colL].IsNull(rawLeftIndex[i]) {
			rightRelsNull = true
		} else {
			if dictLeaf, ok := leftDict[v]; ok {
				rightSegLen := dictLeaf.rightIndexE - dictLeaf.rightIndexS

				if dictLeaf.rightIndexS < dictLeaf.rightIndexE {
					rightRowMapStart := len(rightSrcRowMap)

					leftSrcRowMap = leftSrcRowMap[:len(leftSrcRowMap)+(rightSegLen-1)]
					rightSrcRowMap = rightSrcRowMap[:rightRowMapStart+rightSegLen]
					rightDstRowMap = rightDstRowMap[:rightRowMapStart+rightSegLen]

					for j := dictLeaf.rightIndexS; j < dictLeaf.rightIndexE; j++ {
						currRightSegCount := j - dictLeaf.rightIndexS

						k := i + ext + currRightSegCount

						if currRightSegCount > 0 {
							leftSrcRowMap[k] = rawLeftIndex[i]
						}

						rightSrcRowMap[rightRowMapStart+currRightSegCount] = rawRightIndex[j]
						rightDstRowMap[rightRowMapStart+currRightSegCount] = k

						rightRepeatCount := rightSegLen*dictLeaf.used + currRightSegCount

						// NOTE: Delete surplus object graph relationships. This is a duplicate record when walk.
						if dictLeaf.numOfUniqueRel*rightSegLen <= rightRepeatCount {
							rawTopRelVec[k] = -1

							// TODO: If options.IsObjectGraph is set:
							//         Set null to right columns; resize `rightSrcRowMap` and `rightDstRowMap`, adjust write index of them in this loop.
							//         If currRightSegCount > 0, shrink result table length.
						}
					}
					ext += rightSegLen - 1
				} else {
					rightRelsNull = true
				}

				dictLeaf.used++
				leftDict[v] = dictLeaf
			} else {
				// NOTE: Maybe not reached here.
				rightRelsNull = true
			}
		}

		if rightRelsNull {
			for c := 0; c <= rightRelsLen; c++ {
				retDt.relations[leftRelsLen+leftRelsIncl+c].vec.Set(i+ext, -1)
			}
		}
	}

	rightRowMapStart := len(rightSrcRowMap)
	rightSrcRowMap = rightSrcRowMap[:len(rightSrcRowMap)+len(rightExtra)]
	rightDstRowMap = rightDstRowMap[:len(rightDstRowMap)+len(rightExtra)]

	for i, j := range rightExtra {
		k := leftLen + i + ext

		rightSrcRowMap[rightRowMapStart+i] = rawRightIndex[j]
		rightDstRowMap[rightRowMapStart+i] = k

		for c := 0; c < leftRelsLen; c++ {
			retDt.relations[c].vec.Set(k, -1)
		}
	}

	for c := 0; c < leftColsLen; c++ {
		retDt.cols[c].FillByRowMap(nil, p.cols[c], leftSrcRowMap)
	}
	for c := 0; c < rightColsLen; c++ {
		retDt.cols[leftColsLen+c].FillByRowMap(rightDstRowMap, dtR.cols[c], rightSrcRowMap)
	}
	for c := 0; c < leftRelsLen+leftRelsIncl; c++ {
		if c < len(p.relations) {
			retDt.relations[c].vec.FillByRowMap(nil, p.relations[c].vec, leftSrcRowMap)
		} else {
			break
		}
	}
	if leftRelsIncl > 0 {
		retDt.relations[leftRelsLen].vec.FillByRowMap(nil, p.index, leftSrcRowMap)
	}
	for c := 0; c < rightRelsLen; c++ {
		retDt.relations[leftRelsLen+leftRelsIncl+c].vec.FillByRowMap(rightDstRowMap, dtR.relations[c].vec, rightSrcRowMap)
	}

	return &retDt, nil
}

type joinLeftDictValue struct {
	count          int
	leftIndex      int
	rightIndexS    int
	rightIndexE    int
	used           int
	numOfUniqueRel int
}

type joinLeftKeyAndRelPair struct {
	key interface{}
	rel int
}

func (p *DataTable) initJoinResultDt(
	colL int, dtR *DataTable, colR int, relName string, extendLen int,
	options JoinOptions, isInnerJoin bool) *DataTable {

	leftLen := p.Len()
	leftColsLen := len(p.cols)
	leftRelsLen := len(p.relations)
	leftRelsIncl := 0

	if leftRelsLen == 0 {
		leftRelsIncl = 1
	}

	rightColsLen := len(dtR.cols)
	rightRelsLen := len(dtR.relations)

	retDt := DataTable{
		is3VL:        p.is3VL,
		header:       make([]dtHeader, leftColsLen+rightColsLen),
		cols:         make([]AnyDataColumn, leftColsLen+rightColsLen),
		index:        datacolimpl.NewDataColumnImplWithSize[int](leftLen+extendLen, leftLen+extendLen, Type_Int),
		indexChanged: false,
		rtCtx:        p.rtCtx,
		relations:    make([]dtRelation, 0, leftRelsLen+leftRelsIncl+rightRelsLen+1),
	}

	for i := 0; i < leftRelsLen+leftRelsIncl+rightRelsLen+1; i++ {
		retDt.relations = append(retDt.relations, dtRelation{
			namespace: nil,
			vec:       datacolimpl.NewDataColumnImplWithSize[int](leftLen+extendLen, leftLen+extendLen, Type_Int),
		})
	}

	rawRetIndex := retDt.index.GetRawValues()
	rawRelVec := retDt.relations[leftRelsLen+leftRelsIncl+rightRelsLen].vec.GetRawValues()

	for i := 0; i < len(rawRetIndex); i++ {
		rawRetIndex[i] = i
		rawRelVec[i] = i
	}
	if leftRelsIncl > 0 {
		rawLeftVec := retDt.relations[leftRelsLen+leftRelsIncl+rightRelsLen].vec.GetRawValues()
		for i := 0; i < len(rawRetIndex); i++ {
			rawLeftVec[i] = i
		}
	}

	for c := 0; c < leftColsLen; c++ {
		retDt.header[c] = p.header[c]

		if p.header[c].relIndex < 0 {
			// Set current index (left tail)
			retDt.header[c].relIndex = leftRelsLen + leftRelsIncl - 1
		}

		retDt.cols[c] = datacolumn.NewDataColumnWithSize(leftLen+extendLen, p.Cap()+extendLen, p.cols[c].GetType())
	}

	for c := 0; c < rightColsLen; c++ {
		retDt.header[leftColsLen+c] = dtR.header[c]

		retDt.header[leftColsLen+c].name = p.makeJoinedFieldName(colL, dtR, c, relName)

		if dtR.header[c].parentColIndex >= 0 {
			// Shift index
			retDt.header[leftColsLen+c].parentColIndex = dtR.header[c].parentColIndex + leftColsLen
		} else {
			// Set current index (left primary key column)
			retDt.header[leftColsLen+c].parentColIndex = colL
		}

		if dtR.header[c].relIndex >= 0 {
			// Shift index
			retDt.header[leftColsLen+c].relIndex = dtR.header[c].relIndex + leftRelsLen + leftRelsIncl
		} else {
			// Set current index (result tail)
			retDt.header[leftColsLen+c].relIndex = leftRelsLen + leftRelsIncl + rightRelsLen
		}

		retDt.cols[leftColsLen+c] = datacolumn.NewDataColumnWithSize(leftLen+extendLen, p.Cap()+extendLen, dtR.cols[c].GetType())
	}

	leftNamespace := nameutil.GetNamespaceFromName(p.header[colL].name)

	for c := 0; c < leftRelsLen; c++ {
		retDt.relations[c].parentRelIndex = p.relations[c].parentRelIndex
		if p.relations[c].keyColIndex >= 0 {
			retDt.relations[c].keyColIndex = p.relations[c].keyColIndex
		} else {
			// Set current index (left primary key column)
			retDt.relations[c].keyColIndex = colL
		}

		if nameutil.IsValidName(p.relations[c].namespace) {
			retDt.relations[c].namespace = p.relations[c].namespace
		} else {
			retDt.relations[c].namespace = leftNamespace
		}

		retDt.relations[c].OneToOne = p.relations[c].OneToOne
		retDt.relations[c].Required = p.relations[c].Required
	}

	// left tail
	if leftRelsIncl > 0 {
		retDt.relations[leftRelsLen+leftRelsIncl-1].parentRelIndex = -1
		retDt.relations[leftRelsLen+leftRelsIncl-1].namespace = leftNamespace
		retDt.relations[leftRelsLen+leftRelsIncl-1].OneToOne = false
		retDt.relations[leftRelsLen+leftRelsIncl-1].Required = false
	}

	currentRelsIndex := p.header[colL].relIndex
	if currentRelsIndex < 0 {
		currentRelsIndex = leftRelsLen + leftRelsIncl - 1
	}

	for c := 0; c < rightRelsLen; c++ {
		if dtR.relations[c].parentRelIndex >= 0 {
			// Shift index
			retDt.relations[leftRelsLen+leftRelsIncl+c].parentRelIndex = dtR.relations[c].parentRelIndex + leftRelsLen + leftRelsIncl
		} else {
			// Set current index
			retDt.relations[leftRelsLen+leftRelsIncl+c].parentRelIndex = currentRelsIndex
		}

		if dtR.relations[c].keyColIndex >= 0 {
			// Shift index
			retDt.relations[leftRelsLen+leftRelsIncl+c].keyColIndex = dtR.relations[c].keyColIndex + leftColsLen
		} else {
			// Set current index (right foreign key column)
			retDt.relations[leftRelsLen+leftRelsIncl+c].keyColIndex = colR + leftColsLen
		}

		retDt.relations[leftRelsLen+leftRelsIncl+c].namespace = nameutil.MakeJoinedFieldName(p.header[colL].name, relName, dtR.relations[c].namespace)
		retDt.relations[leftRelsLen+leftRelsIncl+c].OneToOne = dtR.relations[c].OneToOne
		retDt.relations[leftRelsLen+leftRelsIncl+c].Required = dtR.relations[c].Required
	}

	// entired tail
	entiredTailRelsIndex := leftRelsLen + leftRelsIncl + rightRelsLen
	retDt.relations[entiredTailRelsIndex].parentRelIndex = currentRelsIndex
	retDt.relations[entiredTailRelsIndex].keyColIndex = colR + leftColsLen
	retDt.relations[entiredTailRelsIndex].namespace = nameutil.MakeJoinedFieldName(p.header[colL].name, relName, nil)
	retDt.relations[entiredTailRelsIndex].OneToOne = options.RightMustZeroOrOnce
	retDt.relations[entiredTailRelsIndex].Required = isInnerJoin

	return &retDt
}

// makeJoinedName makes a new name for the joined columns of the right table.
func (p *DataTable) makeJoinedFieldName(colL int, dtR *DataTable, c int, relName string) []string {
	return nameutil.MakeJoinedFieldName(p.header[colL].name, relName, dtR.header[c].name)
}
